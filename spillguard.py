"""Pod-wide guard on DuckDB spill disk and memory (#428).

Each DuckDB connection is its own database instance, with its own
max_temp_directory_size and memory_limit. Both caps are per instance, so two
heavy queries at once can together fill the pod's ephemeral storage (and get it
evicted) or its memory (and get it OOM-killed, or starve the node, which the
2Gi request hides from the scheduler). Either takes every query on the pod with
it.

A watchdog thread checks the pod as a whole once a second and acts in two steps:

- Drain: the pod is under pressure. /readyz fails, so the Service sends new
  requests to other replicas, and new queries that still arrive are turned away
  with a retry message.
- Interrupt: it interrupts the one connection using the most of the resource
  that is short. That query fails with a message saying why, DuckDB frees what
  it held, and everything else on the pod keeps running.

Spill disk is measured as the kubelet does, by allocated blocks under the
duckdb-* directories, against POD_EPHEMERAL_LIMIT (or SPILL_GUARD_LIMIT): drain
at SPILL_DRAIN_FRACTION (0.5), interrupt at SPILL_INTERRUPT_FRACTION (0.8).

Memory has two signals. The pod's own working set (cgroup memory.current less
inactive page cache) against its limit (memory.max, else POD_MEMORY_LIMIT):
drain at MEMORY_DRAIN_FRACTION (0.5), interrupt at MEMORY_INTERRUPT_FRACTION
(0.9), short of the OOM kill. And the node's MemAvailable from /proc/meminfo,
which a container sees for the whole node: drain below NODE_MEMORY_DRAIN_FLOOR
(16GiB), interrupt below NODE_MEMORY_INTERRUPT_FLOOR (8GiB). The node signal
counts only while the pod itself holds at least NODE_MEMORY_MIN_POD (8GiB), so
an idle pod is not drained for other tenants' use. Per-connection memory comes
from duckdb_memory(), read through a cursor opened and closed for each reading.

Pressure clears once usage falls to 80% of the drain mark (for the node: once
available memory is 25% above the floor). With no disk limit the disk half is
off; MEMORY_GUARD=0 turns the memory half off.
"""
import os
import sys
import threading
import itertools
import time
import weakref
from typing import NamedTuple

from dbconfig import _parse_bytes

_CLEAR_RATIO = 0.8
_NODE_CLEAR_RATIO = 1.25
# Bytes of a connection interrupted for memory count as on their way out until
# _FREEING_GRACE after it unregisters (the allocator and cgroup lag the free),
# or _FREEING_TTL after the interrupt if it never unregisters (a tile connection).
# Without the grace, the next reading could stop a second query for memory the
# first is still returning.
_FREEING_TTL = 30.0
_FREEING_GRACE = 5.0


def _fraction(var: str, default: float) -> float:
    try:
        return float(os.environ.get(var, "").strip() or default)
    except ValueError:
        return default


def _env_bytes(var: str, default: int) -> int:
    raw = os.environ.get(var, "").strip()
    if not raw:
        return default
    try:
        return _parse_bytes(raw)
    except ValueError:
        print(f"⚠️ {var}={raw!r} is not a size; using {default >> 30} GiB", file=sys.stderr)
        return default


def spill_root() -> str:
    return os.environ.get("DUCKDB_TEMP_ROOT", "").strip() or "/tmp"


def _limit_bytes() -> int | None:
    raw = (os.environ.get("SPILL_GUARD_LIMIT", "").strip()
           or os.environ.get("POD_EPHEMERAL_LIMIT", "").strip())
    if not raw:
        return None
    try:
        value = _parse_bytes(raw)
    except ValueError as e:
        print(f"⚠️ spill guard off: {e}", file=sys.stderr)
        return None
    return value or None


def _dir_usage(path: str) -> int:
    """Bytes allocated under *path*, as du and the kubelet count them."""
    total = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        total += _dir_usage(entry.path)
                    else:
                        total += entry.stat(follow_symlinks=False).st_blocks * 512
                except OSError:
                    continue  # removed while scanning
    except OSError:
        return 0
    return total


class MemorySample(NamedTuple):
    pod_used: int | None        # working set: what the OOM killer and kubelet count
    pod_limit: int | None
    node_available: int | None  # MemAvailable for the whole node


def read_memory(cgroup: str = "/sys/fs/cgroup", meminfo: str = "/proc/meminfo") -> MemorySample:
    """The pod's working set and limit (cgroup v2) and the node's available memory."""
    used = limit = available = None
    try:
        with open(os.path.join(cgroup, "memory.current")) as f:
            current = int(f.read())
        inactive_file = 0
        with open(os.path.join(cgroup, "memory.stat")) as f:
            for line in f:
                if line.startswith("inactive_file "):
                    inactive_file = int(line.split()[1])
                    break
        used = max(current - inactive_file, 0)
    except (OSError, ValueError):
        pass
    try:
        with open(os.path.join(cgroup, "memory.max")) as f:
            raw = f.read().strip()
        limit = None if raw == "max" else int(raw)
    except (OSError, ValueError):
        pass
    if limit is None:
        raw = os.environ.get("POD_MEMORY_LIMIT", "").strip()
        try:
            limit = (_parse_bytes(raw) or None) if raw else None
        except ValueError:
            limit = None
    try:
        with open(meminfo) as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    available = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError):
        pass
    return MemorySample(used, limit, available)


def instance_memory(conn) -> int | None:
    """Bytes DuckDB holds for *conn*'s database instance, read while it runs.

    A cursor is a second connection to the same instance, so it can read
    duckdb_memory() during a query. It is closed at once: an open cursor would
    keep the instance, its memory and its spill files alive after the query's own
    connection closes.
    """
    try:
        cur = conn.cursor()
    except Exception:
        return None  # closed
    try:
        return int(cur.sql("SELECT coalesce(sum(memory_usage_bytes), 0) "
                           "FROM duckdb_memory()").fetchone()[0])
    except Exception:
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass


class SpillGuard:
    def __init__(self, limit: int | None = None, root: str | None = None,
                 drain_fraction: float | None = None,
                 interrupt_fraction: float | None = None,
                 memory: bool | None = None, memory_reader=None,
                 memory_drain_fraction: float | None = None,
                 memory_interrupt_fraction: float | None = None,
                 node_drain_floor: int | None = None,
                 node_interrupt_floor: int | None = None,
                 node_min_pod: int | None = None,
                 memory_min_victim: int | None = None,
                 spill_min_victim: int | None = None):
        self.limit = limit
        self.root = root or spill_root()
        self.drain_at = (limit * (drain_fraction if drain_fraction is not None
                                  else _fraction("SPILL_DRAIN_FRACTION", 0.5))
                         if limit else None)
        self.interrupt_at = (limit * (interrupt_fraction if interrupt_fraction is not None
                                      else _fraction("SPILL_INTERRUPT_FRACTION", 0.8))
                             if limit else None)
        # Smallest spill worth stopping: anything less frees too little to matter.
        self.spill_min_victim = (spill_min_victim if spill_min_victim is not None
                                 else _env_bytes("SPILL_MIN_VICTIM", 1 << 30))
        if memory is None:
            memory = os.environ.get("MEMORY_GUARD", "1").strip().lower() not in ("0", "false", "no", "off")
        self.memory_enabled = memory
        self._read_memory = memory_reader or read_memory
        self.memory_drain_fraction = (memory_drain_fraction if memory_drain_fraction is not None
                                      else _fraction("MEMORY_DRAIN_FRACTION", 0.5))
        self.memory_interrupt_fraction = (memory_interrupt_fraction if memory_interrupt_fraction is not None
                                          else _fraction("MEMORY_INTERRUPT_FRACTION", 0.9))
        self.node_drain_floor = (node_drain_floor if node_drain_floor is not None
                                 else _env_bytes("NODE_MEMORY_DRAIN_FLOOR", 16 << 30))
        self.node_interrupt_floor = (node_interrupt_floor if node_interrupt_floor is not None
                                     else _env_bytes("NODE_MEMORY_INTERRUPT_FLOOR", 8 << 30))
        self.node_min_pod = (node_min_pod if node_min_pod is not None
                             else _env_bytes("NODE_MEMORY_MIN_POD", 8 << 30))
        # Smallest instance worth stopping for memory: anything less frees too
        # little to matter, so the guard looks past it.
        self.memory_min_victim = (memory_min_victim if memory_min_victim is not None
                                  else _env_bytes("MEMORY_MIN_VICTIM", 1 << 30))
        self._lock = threading.Lock()
        self._conns: dict[str, weakref.ref] = {}
        # Spill directory -> registration order, so the guard can stop the
        # newest query first.
        self._order: dict[str, int] = {}
        self._seq = itertools.count()
        # Spill directory -> (time.monotonic() of the interrupt, "disk" | "memory").
        # Kept past unregister so the query can still tell why it failed; pruned
        # after a minute.
        self._interrupted: dict[str, tuple[float, str]] = {}
        # Spill directory -> (expiry on time.monotonic(), bytes) for connections
        # interrupted for memory, counted as freeing until then.
        self._memory_freeing: dict[str, tuple[float, int]] = {}
        self._disk_pressure = False
        self._memory_pressure: str | None = None
        self.last_total = 0
        self.last_memory: MemorySample | None = None

    @property
    def disk_enabled(self) -> bool:
        return self.limit is not None

    @property
    def enabled(self) -> bool:
        return self.disk_enabled or self.memory_enabled

    def register(self, conn) -> str | None:
        """Track *conn* by its spill directory, so the guard can interrupt it."""
        try:
            temp_dir = conn.sql("SELECT current_setting('temp_directory')").fetchone()[0]
        except Exception:
            return None
        if temp_dir:
            with self._lock:
                d = os.path.normpath(temp_dir)
                self._conns[d] = weakref.ref(conn)
                self._order[d] = next(self._seq)
        return temp_dir

    def unregister(self, temp_dir: str | None) -> None:
        if temp_dir:
            with self._lock:
                d = os.path.normpath(temp_dir)
                self._conns.pop(d, None)
                self._order.pop(d, None)
                if d in self._memory_freeing:
                    expiry, nbytes = self._memory_freeing[d]
                    self._memory_freeing[d] = (min(expiry, time.monotonic() + _FREEING_GRACE), nbytes)

    def was_interrupted(self, temp_dir: str | None) -> str | None:
        """Why the guard interrupted this connection ("disk" or "memory"), or
        None if it did not. Consumes the record."""
        if not temp_dir:
            return None
        with self._lock:
            record = self._interrupted.pop(os.path.normpath(temp_dir), None)
        return record[1] if record else None

    def _prune(self) -> None:
        now = time.monotonic()
        with self._lock:
            for d in [d for d, ref in self._conns.items() if ref() is None]:
                del self._conns[d]
                self._order.pop(d, None)
            for d in [d for d, (t, _) in self._interrupted.items() if now - t > 60]:
                del self._interrupted[d]
            for d in [d for d, (expiry, _) in self._memory_freeing.items() if now > expiry]:
                del self._memory_freeing[d]

    def under_pressure(self) -> bool:
        return self._disk_pressure or self._memory_pressure is not None

    def pressure_reason(self) -> str | None:
        if self._disk_pressure:
            return "spill disk under pressure"
        if self._memory_pressure:
            return f"{self._memory_pressure} memory under pressure"
        return None

    def usage(self) -> dict[str, int]:
        """Allocated bytes per spill directory (duckdb-* under the root)."""
        out = {}
        try:
            with os.scandir(self.root) as it:
                for entry in it:
                    if entry.name.startswith("duckdb-") and entry.is_dir(follow_symlinks=False):
                        out[os.path.normpath(entry.path)] = _dir_usage(entry.path)
        except OSError:
            pass
        return out

    def check(self) -> None:
        """One watchdog pass over disk and memory."""
        if not self.enabled:
            return
        self._prune()
        if self.disk_enabled:
            self._check_disk()
        if self.memory_enabled:
            self._check_memory()

    @staticmethod
    def _pick(candidates: list[tuple[int, int, str]], min_victim: int) -> tuple[int, str, str]:
        """Choose which query to stop from (registration order, bytes, dir).

        The newest query that holds at least *min_victim*: older queries have
        done the most work, and each stays within its own limit once the
        newcomer is gone. If that does not free enough, the next check stops the
        next newest. Only when nothing is that big does it take the largest.
        Returns (bytes, dir, which).
        """
        big = [c for c in candidates if c[1] >= min_victim]
        if big:
            _, nbytes, victim = max(big)
            return nbytes, victim, "the newest"
        _, nbytes, victim = max(candidates, key=lambda c: c[1])
        return nbytes, victim, "the largest"

    def _interrupt(self, victim: str, reason: str, nbytes: int, what: str,
                   which: str) -> None:
        with self._lock:
            ref = self._conns.get(victim)
            conn = ref() if ref else None
            self._interrupted[victim] = (time.monotonic(), reason)
            if reason == "memory":
                self._memory_freeing[victim] = (time.monotonic() + _FREEING_TTL, nbytes)
        if conn is None:
            return
        print(f"🛑 {what}; interrupting {which} ({nbytes / 2**30:.1f} GiB, {victim})",
              file=sys.stderr)
        try:
            conn.interrupt()
        except Exception as e:
            print(f"⚠️ guard: interrupt failed: {e}", file=sys.stderr)

    def _check_disk(self) -> None:
        usage = self.usage()
        total = sum(usage.values())
        self.last_total = total
        if total >= self.drain_at:
            if not self._disk_pressure:
                print(f"⚠️ spill guard: {total / 2**30:.1f} GiB spilled of "
                      f"{self.limit / 2**30:.1f} GiB; draining this replica",
                      file=sys.stderr)
            self._disk_pressure = True
        elif self._disk_pressure and total < self.drain_at * _CLEAR_RATIO:
            print(f"✅ spill guard: {total / 2**30:.1f} GiB spilled; serving again",
                  file=sys.stderr)
            self._disk_pressure = False
        # A spill already interrupted is on its way out: DuckDB frees it as the
        # query unwinds. Count only what isn't, or one runaway would get every
        # other spill interrupted during the second or so it takes to clean up.
        with self._lock:
            freeing = sum(size for d, size in usage.items() if d in self._interrupted)
        if total - freeing < self.interrupt_at:
            return
        with self._lock:
            candidates = [(self._order.get(d, -1), size, d) for d, size in usage.items()
                          if d in self._conns and d not in self._interrupted]
        if not candidates:
            return
        size, victim, which = self._pick(candidates, self.spill_min_victim)
        self._interrupt(victim, "disk", size,
                        f"spill guard: {total / 2**30:.1f} GiB spilled of "
                        f"{self.limit / 2**30:.1f} GiB", which)

    def _check_memory(self) -> None:
        sample = self._read_memory()
        self.last_memory = sample
        if sample.pod_used is None:
            return
        # Memory of a query already interrupted is on its way out; as for disk,
        # count it as gone so the guard does not stop a second query meanwhile.
        with self._lock:
            freeing = sum(b for _, b in self._memory_freeing.values())
        used = max(sample.pod_used - freeing, 0)
        limit = sample.pod_limit
        available = (sample.node_available + freeing
                     if sample.node_available is not None else None)
        node_counts = available is not None and used >= self.node_min_pod

        pod_drain = bool(limit) and used >= limit * self.memory_drain_fraction
        node_drain = node_counts and available < self.node_drain_floor
        if pod_drain or node_drain:
            if self._memory_pressure is None:
                print(f"⚠️ memory guard: pod {used / 2**30:.1f} GiB"
                      + (f" of {limit / 2**30:.1f} GiB" if limit else "")
                      + (f", node {available / 2**30:.1f} GiB available" if available is not None else "")
                      + "; draining this replica", file=sys.stderr)
            self._memory_pressure = "pod" if pod_drain else "node"
        elif self._memory_pressure is not None:
            pod_clear = not limit or used < limit * self.memory_drain_fraction * _CLEAR_RATIO
            node_clear = not node_counts or available >= self.node_drain_floor * _NODE_CLEAR_RATIO
            if pod_clear and node_clear:
                print(f"✅ memory guard: pod {used / 2**30:.1f} GiB; serving again",
                      file=sys.stderr)
                self._memory_pressure = None

        pod_critical = bool(limit) and used >= limit * self.memory_interrupt_fraction
        node_critical = node_counts and available < self.node_interrupt_floor
        if not (pod_critical or node_critical):
            return
        with self._lock:
            live = [(self._order.get(d, -1), d, ref()) for d, ref in self._conns.items()
                    if d not in self._interrupted]
        candidates = []
        for order, d, conn in live:
            if conn is None:
                continue
            nbytes = instance_memory(conn)
            if nbytes:
                candidates.append((order, nbytes, d))
        if not candidates:
            return
        nbytes, victim, which = self._pick(candidates, self.memory_min_victim)
        what = (f"memory guard: pod {used / 2**30:.1f} GiB of {limit / 2**30:.1f} GiB"
                if pod_critical else
                f"memory guard: node has {available / 2**30:.1f} GiB available")
        self._interrupt(victim, "memory", nbytes, what, which)

    def start(self, interval: float | None = None) -> threading.Thread | None:
        if not self.enabled:
            print("ℹ️ guard off (no POD_EPHEMERAL_LIMIT or SPILL_GUARD_LIMIT, and MEMORY_GUARD=0)",
                  file=sys.stderr)
            return None
        if interval is None:
            # Two heavy spills together grew ~2.5 GiB between 2s checks on dev, so
            # the interrupt fired 4 GiB past its mark. Checking every second and
            # interrupting at 80% leaves ~8 GiB before the eviction limit (#428).
            interval = _fraction("SPILL_GUARD_INTERVAL", 1.0)

        def loop():
            while True:
                try:
                    self.check()
                except Exception as e:  # the watchdog must not die
                    print(f"⚠️ guard check failed: {type(e).__name__}: {e}",
                          file=sys.stderr)
                time.sleep(interval)

        thread = threading.Thread(target=loop, name="spill-guard", daemon=True)
        thread.start()
        if self.disk_enabled:
            print(f"🛡️ spill guard on: limit {self.limit / 2**30:.1f} GiB, drain at "
                  f"{self.drain_at / 2**30:.1f} GiB, interrupt at "
                  f"{self.interrupt_at / 2**30:.1f} GiB, root {self.root}", file=sys.stderr)
        else:
            print("ℹ️ spill guard off (no POD_EPHEMERAL_LIMIT or SPILL_GUARD_LIMIT)",
                  file=sys.stderr)
        if self.memory_enabled:
            s = self._read_memory()
            pod = (f"pod limit {s.pod_limit / 2**30:.1f} GiB, drain at "
                   f"{self.memory_drain_fraction:.0%}, interrupt at "
                   f"{self.memory_interrupt_fraction:.0%}" if s.pod_limit else "no pod limit")
            print(f"🛡️ memory guard on: {pod}; node floors "
                  f"{self.node_drain_floor / 2**30:.0f}/{self.node_interrupt_floor / 2**30:.0f} GiB "
                  f"while the pod holds {self.node_min_pod / 2**30:.0f} GiB+", file=sys.stderr)
        return thread


GUARD = SpillGuard(limit=_limit_bytes())

BUSY_MESSAGE = (
    "Server busy: this replica is short of memory or temporary disk because other "
    "queries are using it. Nothing was run. Retry the same query; it will go to "
    "another replica."
)

INTERRUPTED_MESSAGE = (
    "Query stopped: it used too much temporary disk on this server and was "
    "interrupted so the server could keep running. Make the query hold less data "
    "at once, for example by filtering or aggregating earlier."
)

MEMORY_INTERRUPTED_MESSAGE = (
    "Query stopped: it used too much memory on this server and was interrupted so "
    "the server could keep running. Make the query hold less data at once, for "
    "example by filtering or aggregating earlier."
)

INTERRUPTED_MESSAGES = {"disk": INTERRUPTED_MESSAGE, "memory": MEMORY_INTERRUPTED_MESSAGE}
