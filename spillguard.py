"""Pod-wide guard on DuckDB spill disk (#428).

Each DuckDB connection spills to its own directory, capped by DuckDB's
max_temp_directory_size. That cap is per connection, and DuckDB's own count runs
about 30% under what the files take on disk, so two heavy queries at once could
fill the pod's ephemeral storage and get it evicted, taking every query on the
pod with it.

A watchdog thread measures the spill directories as the kubelet does, by
allocated blocks, and acts on the pod-wide total:

- At the drain mark (SPILL_DRAIN_FRACTION of the limit, default 0.5) the pod is
  under pressure: /readyz fails, so the Service sends new requests to other
  replicas, and new queries that still arrive are turned away with a retry
  message. Pressure clears once the total falls below 80% of the drain mark.
- At the interrupt mark (SPILL_INTERRUPT_FRACTION, default 0.85) it interrupts
  the connection with the largest spill. That query fails, DuckDB frees its spill
  files, and everything else on the pod keeps running.

The limit is POD_EPHEMERAL_LIMIT (the container's ephemeral-storage limit, from
the Downward API) or SPILL_GUARD_LIMIT. With neither set the guard is off.
"""
import os
import sys
import threading
import time
import weakref

from dbconfig import _parse_bytes

_DRAIN_CLEAR_RATIO = 0.8


def _fraction(var: str, default: float) -> float:
    try:
        return float(os.environ.get(var, "").strip() or default)
    except ValueError:
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


class SpillGuard:
    def __init__(self, limit: int | None = None, root: str | None = None,
                 drain_fraction: float | None = None,
                 interrupt_fraction: float | None = None):
        self.limit = limit
        self.root = root or spill_root()
        self.drain_at = (limit * (drain_fraction if drain_fraction is not None
                                  else _fraction("SPILL_DRAIN_FRACTION", 0.5))
                         if limit else None)
        self.interrupt_at = (limit * (interrupt_fraction if interrupt_fraction is not None
                                      else _fraction("SPILL_INTERRUPT_FRACTION", 0.85))
                             if limit else None)
        self._lock = threading.Lock()
        self._conns: dict[str, weakref.ref] = {}
        # Spill directory -> time.monotonic() of the interrupt. Kept past
        # unregister so the query can still tell why it failed; pruned after a minute.
        self._interrupted: dict[str, float] = {}
        self._pressure = False
        self.last_total = 0

    @property
    def enabled(self) -> bool:
        return self.limit is not None

    def register(self, conn) -> str | None:
        """Track *conn* by its spill directory, so the guard can interrupt it."""
        try:
            temp_dir = conn.sql("SELECT current_setting('temp_directory')").fetchone()[0]
        except Exception:
            return None
        if temp_dir:
            with self._lock:
                self._conns[os.path.normpath(temp_dir)] = weakref.ref(conn)
        return temp_dir

    def unregister(self, temp_dir: str | None) -> None:
        if temp_dir:
            with self._lock:
                self._conns.pop(os.path.normpath(temp_dir), None)

    def was_interrupted(self, temp_dir: str | None) -> bool:
        """Whether the guard interrupted this connection. Consumes the record."""
        if not temp_dir:
            return False
        with self._lock:
            return self._interrupted.pop(os.path.normpath(temp_dir), None) is not None

    def _prune(self) -> None:
        now = time.monotonic()
        with self._lock:
            for d in [d for d, ref in self._conns.items() if ref() is None]:
                del self._conns[d]
            for d in [d for d, t in self._interrupted.items() if now - t > 60]:
                del self._interrupted[d]

    def under_pressure(self) -> bool:
        return self._pressure

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
        """One watchdog pass: update pressure, interrupt the largest spiller if needed."""
        if not self.enabled:
            return
        self._prune()
        usage = self.usage()
        total = sum(usage.values())
        self.last_total = total
        if total >= self.drain_at:
            if not self._pressure:
                print(f"⚠️ spill guard: {total / 2**30:.1f} GiB spilled of "
                      f"{self.limit / 2**30:.1f} GiB; draining this replica",
                      file=sys.stderr)
            self._pressure = True
        elif self._pressure and total < self.drain_at * _DRAIN_CLEAR_RATIO:
            print(f"✅ spill guard: {total / 2**30:.1f} GiB spilled; serving again",
                  file=sys.stderr)
            self._pressure = False
        # A spill already interrupted is on its way out: DuckDB frees it as the
        # query unwinds. Count only what isn't, or one runaway would get every
        # other spill interrupted during the second or so it takes to clean up.
        with self._lock:
            freeing = sum(size for d, size in usage.items() if d in self._interrupted)
        if total - freeing < self.interrupt_at:
            return
        with self._lock:
            candidates = [(size, d) for d, size in usage.items()
                          if d in self._conns and d not in self._interrupted]
        if not candidates:
            return
        size, victim = max(candidates)
        with self._lock:
            conn = self._conns[victim]()
            self._interrupted[victim] = time.monotonic()
        if conn is None:
            return
        print(f"🛑 spill guard: {total / 2**30:.1f} GiB spilled of "
              f"{self.limit / 2**30:.1f} GiB; interrupting the largest spill "
              f"({size / 2**30:.1f} GiB, {victim})", file=sys.stderr)
        try:
            conn.interrupt()
        except Exception as e:
            print(f"⚠️ spill guard: interrupt failed: {e}", file=sys.stderr)

    def start(self, interval: float | None = None) -> threading.Thread | None:
        if not self.enabled:
            print("ℹ️ spill guard off (no POD_EPHEMERAL_LIMIT or SPILL_GUARD_LIMIT)",
                  file=sys.stderr)
            return None
        if interval is None:
            interval = _fraction("SPILL_GUARD_INTERVAL", 2.0)

        def loop():
            while True:
                try:
                    self.check()
                except Exception as e:  # the watchdog must not die
                    print(f"⚠️ spill guard check failed: {type(e).__name__}: {e}",
                          file=sys.stderr)
                time.sleep(interval)

        thread = threading.Thread(target=loop, name="spill-guard", daemon=True)
        thread.start()
        print(f"🛡️ spill guard on: limit {self.limit / 2**30:.1f} GiB, drain at "
              f"{self.drain_at / 2**30:.1f} GiB, interrupt at "
              f"{self.interrupt_at / 2**30:.1f} GiB, root {self.root}", file=sys.stderr)
        return thread


GUARD = SpillGuard(limit=_limit_bytes())

BUSY_MESSAGE = (
    "Server busy: this replica is low on temporary disk because other queries are "
    "spilling. Nothing was run. Retry the same query; it will go to another replica."
)

INTERRUPTED_MESSAGE = (
    "Query stopped: it used too much temporary disk on this server and was "
    "interrupted so the server could keep running. Make the query hold less data "
    "at once, for example by filtering or aggregating earlier."
)
