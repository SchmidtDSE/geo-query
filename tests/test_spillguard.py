"""Pod-wide spill guard (#428)."""
import os
import threading
import time

import pytest

from spillguard import MemorySample, SpillGuard, instance_memory, read_memory
from tests.test_dbconfig import _SPILL_QUERY, _spill_conn


def _fill(path, nbytes):
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "duckdb_temp_storage-0.tmp"), "wb") as f:
        f.write(os.urandom(nbytes))


def test_off_without_a_limit(tmp_path):
    guard = SpillGuard(limit=None, root=str(tmp_path), memory=False)
    assert not guard.enabled
    guard.check()
    assert not guard.under_pressure()
    assert guard.start() is None


def test_usage_counts_only_duckdb_spill_dirs(tmp_path):
    _fill(tmp_path / "duckdb-a", 1 << 20)
    _fill(tmp_path / "other", 1 << 20)
    guard = SpillGuard(memory=False, limit=10 << 20, root=str(tmp_path))
    usage = guard.usage()
    assert list(usage) == [os.path.normpath(tmp_path / "duckdb-a")]
    assert usage[os.path.normpath(tmp_path / "duckdb-a")] >= 1 << 20


def test_pressure_turns_on_at_the_drain_mark_and_off_below_it(tmp_path):
    guard = SpillGuard(memory=False, limit=10 << 20, root=str(tmp_path),
                       drain_fraction=0.5, interrupt_fraction=0.99)
    _fill(tmp_path / "duckdb-a", 6 << 20)
    guard.check()
    assert guard.under_pressure()
    os.remove(tmp_path / "duckdb-a" / "duckdb_temp_storage-0.tmp")
    _fill(tmp_path / "duckdb-a", 9 << 19)  # 4.5 MiB: under the mark, above 80% of it
    guard.check()
    assert guard.under_pressure()
    os.remove(tmp_path / "duckdb-a" / "duckdb_temp_storage-0.tmp")
    guard.check()
    assert not guard.under_pressure()


class _FakeConn:
    def __init__(self, temp_dir):
        self.temp_dir, self.interrupted = temp_dir, False

    def sql(self, _q):
        temp_dir = self.temp_dir

        class R:
            def fetchone(self):
                return (temp_dir,)
        return R()

    def interrupt(self):
        self.interrupted = True

    def cursor(self):
        mem = getattr(self, "memory", 0)

        class C:
            def sql(self, _q):
                class R:
                    def fetchone(self):
                        return (mem,)
                return R()

            def close(self):
                pass
        return C()


def test_interrupts_the_largest_registered_spill_once(tmp_path):
    guard = SpillGuard(memory=False, limit=10 << 20, root=str(tmp_path),
                       drain_fraction=0.5, interrupt_fraction=0.8)
    small, big = _FakeConn(str(tmp_path / "duckdb-small")), _FakeConn(str(tmp_path / "duckdb-big"))
    guard.register(small)
    guard.register(big)
    _fill(small.temp_dir, 2 << 20)
    _fill(big.temp_dir, 7 << 20)
    guard.check()
    assert big.interrupted and not small.interrupted
    big.interrupted = False
    guard.check()  # still over on disk, but the big spill is already being freed
    assert not big.interrupted and not small.interrupted
    _fill(small.temp_dir, 9 << 20)  # the small one grows past the mark on its own
    guard.check()
    assert small.interrupted and not big.interrupted
    assert guard.was_interrupted(big.temp_dir)
    assert not guard.was_interrupted(big.temp_dir)  # consumed


def test_unregistered_spill_is_never_interrupted(tmp_path):
    guard = SpillGuard(memory=False, limit=10 << 20, root=str(tmp_path), interrupt_fraction=0.5)
    conn = _FakeConn(str(tmp_path / "duckdb-x"))
    guard.register(conn)
    guard.unregister(conn.temp_dir)
    _fill(conn.temp_dir, 8 << 20)
    guard.check()
    assert not conn.interrupted


def test_real_concurrent_spills_stop_one_query_not_the_process(monkeypatch, tmp_path):
    """Two DuckDB queries spill at once. The guard interrupts the larger before the
    pod-wide total passes its mark; the other runs to completion."""
    duckdb = pytest.importorskip("duckdb")
    monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
    monkeypatch.delenv("POD_EPHEMERAL_LIMIT", raising=False)
    monkeypatch.delenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", raising=False)
    # Measure how much one copy spills, then set the limit so one fits and two don't.
    probe = SpillGuard(memory=False, limit=1 << 40, root=str(tmp_path))
    peak = [0]
    done = threading.Event()

    def watch():
        while not done.is_set():
            peak[0] = max(peak[0], sum(probe.usage().values()))
            time.sleep(0.05)

    w = threading.Thread(target=watch)
    w.start()
    con = _spill_conn(duckdb)
    expected = con.sql(_SPILL_QUERY).fetchall()
    con.close()
    done.set()
    w.join()
    assert peak[0] > 0, "the query did not spill"

    guard = SpillGuard(memory=False, limit=int(peak[0] * 1.6), root=str(tmp_path),
                       drain_fraction=0.3, interrupt_fraction=0.75)
    stop = threading.Event()

    def watchdog():
        while not stop.is_set():
            guard.check()
            time.sleep(0.05)

    results, errors = [], []

    def run():
        c = _spill_conn(duckdb)
        guard.register(c)
        try:
            results.append(c.sql(_SPILL_QUERY).fetchall())
        except duckdb.InterruptException as e:
            errors.append(e)
        finally:
            c.close()

    wd = threading.Thread(target=watchdog)
    wd.start()
    ts = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    stop.set()
    wd.join()
    assert len(errors) == 1 and results == [expected]
    assert list(tmp_path.iterdir()) == []


def test_default_marks_leave_room_before_eviction(monkeypatch, tmp_path):
    """Dev (#428): two spills grew ~2.5 GiB between 2s checks, so the defaults
    interrupt at 80% and check every second."""
    for var in ("SPILL_DRAIN_FRACTION", "SPILL_INTERRUPT_FRACTION", "SPILL_GUARD_INTERVAL"):
        monkeypatch.delenv(var, raising=False)
    guard = SpillGuard(memory=False, limit=50 << 30, root=str(tmp_path))
    assert guard.drain_at == 25 << 30
    assert guard.interrupt_at == 40 << 30


# --- memory -----------------------------------------------------------------

_GiB = 1 << 30


def test_read_memory_parses_cgroup_and_meminfo(monkeypatch, tmp_path):
    (tmp_path / "memory.current").write_text(str(10 * _GiB))
    (tmp_path / "memory.stat").write_text(f"anon 1\ninactive_file {2 * _GiB}\nactive_file 5\n")
    (tmp_path / "memory.max").write_text(str(160 * _GiB))
    (tmp_path / "meminfo").write_text("MemTotal: 999 kB\nMemAvailable:    1048576 kB\n")
    s = read_memory(str(tmp_path), str(tmp_path / "meminfo"))
    assert s == MemorySample(8 * _GiB, 160 * _GiB, _GiB)
    # No cgroup limit: fall back to the Downward API value.
    (tmp_path / "memory.max").write_text("max\n")
    monkeypatch.setenv("POD_MEMORY_LIMIT", str(32 * _GiB))
    assert read_memory(str(tmp_path), str(tmp_path / "meminfo")).pod_limit == 32 * _GiB
    monkeypatch.delenv("POD_MEMORY_LIMIT")
    assert read_memory(str(tmp_path / "missing"), str(tmp_path / "nope")) == MemorySample(None, None, None)


def _mem_guard(sample, **kw):
    box = [sample]
    guard = SpillGuard(limit=None, root="/nonexistent", memory=True,
                       memory_reader=lambda: box[0], **kw)
    return guard, box


def test_pod_memory_drains_and_clears():
    guard, box = _mem_guard(MemorySample(60 * _GiB, 100 * _GiB, 400 * _GiB))
    guard.check()
    assert guard.under_pressure() and guard.pressure_reason() == "pod memory under pressure"
    box[0] = MemorySample(45 * _GiB, 100 * _GiB, 400 * _GiB)  # under the mark, above 80% of it
    guard.check()
    assert guard.under_pressure()
    box[0] = MemorySample(30 * _GiB, 100 * _GiB, 400 * _GiB)
    guard.check()
    assert not guard.under_pressure()


def test_node_memory_counts_only_while_the_pod_holds_some():
    # An idle pod on a busy node is not drained for other tenants' use.
    guard, box = _mem_guard(MemorySample(1 * _GiB, 160 * _GiB, 4 * _GiB))
    guard.check()
    assert not guard.under_pressure()
    # A pod holding 20 GiB on a node with 12 GiB left drains.
    box[0] = MemorySample(20 * _GiB, 160 * _GiB, 12 * _GiB)
    guard.check()
    assert guard.pressure_reason() == "node memory under pressure"
    box[0] = MemorySample(20 * _GiB, 160 * _GiB, 18 * _GiB)  # above the floor, not yet 25% above
    guard.check()
    assert guard.under_pressure()
    box[0] = MemorySample(20 * _GiB, 160 * _GiB, 21 * _GiB)
    guard.check()
    assert not guard.under_pressure()


def test_memory_interrupts_the_newest_big_instance_once(tmp_path):
    guard, box = _mem_guard(MemorySample(95 * _GiB, 100 * _GiB, 400 * _GiB))
    old, new = _FakeConn(str(tmp_path / "duckdb-o")), _FakeConn(str(tmp_path / "duckdb-n"))
    old.memory, new.memory = 70 * _GiB, 20 * _GiB
    guard.register(old)
    guard.register(new)
    guard.check()
    assert new.interrupted and not old.interrupted
    new.interrupted = False
    box[0] = MemorySample(110 * _GiB, 100 * _GiB, 400 * _GiB)  # 90 after the 20 freeing
    guard.check()
    assert old.interrupted, "still critical once the newest is counted as freed"
    assert guard.was_interrupted(new.temp_dir) == "memory"


def test_memory_stops_only_one_while_it_frees(tmp_path):
    guard, _ = _mem_guard(MemorySample(95 * _GiB, 100 * _GiB, 400 * _GiB))
    old, new = _FakeConn(str(tmp_path / "duckdb-o")), _FakeConn(str(tmp_path / "duckdb-n"))
    old.memory, new.memory = 70 * _GiB, 20 * _GiB
    guard.register(old)
    guard.register(new)
    guard.check()
    guard.check()  # 95 - 20 freeing = 75, under the mark
    assert new.interrupted and not old.interrupted
    assert guard.was_interrupted(old.temp_dir) is None


def test_memory_looks_past_a_newest_too_small_to_matter(tmp_path):
    guard, _ = _mem_guard(MemorySample(95 * _GiB, 100 * _GiB, 400 * _GiB))
    old, mid, tiny = (_FakeConn(str(tmp_path / f"duckdb-{n}")) for n in "omt")
    old.memory, mid.memory, tiny.memory = 50 * _GiB, 30 * _GiB, 200 << 20
    for c in (old, mid, tiny):
        guard.register(c)
    guard.check()
    assert mid.interrupted and not old.interrupted and not tiny.interrupted


def test_memory_falls_back_to_the_largest_when_none_is_big(tmp_path):
    guard, _ = _mem_guard(MemorySample(95 * _GiB, 100 * _GiB, 400 * _GiB),
                          memory_min_victim=1 * _GiB)
    a, b = _FakeConn(str(tmp_path / "duckdb-a")), _FakeConn(str(tmp_path / "duckdb-b"))
    a.memory, b.memory = 800 << 20, 300 << 20
    guard.register(a)
    guard.register(b)
    guard.check()
    assert a.interrupted and not b.interrupted


def test_node_floor_interrupts_too(tmp_path):
    guard, _ = _mem_guard(MemorySample(40 * _GiB, 160 * _GiB, 5 * _GiB))
    conn = _FakeConn(str(tmp_path / "duckdb-x"))
    conn.memory = 30 * _GiB
    guard.register(conn)
    guard.check()
    assert conn.interrupted


def test_instance_memory_reads_a_running_query_and_skips_a_closed_one():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.sql("SET threads=2; SET memory_limit='300MB'; SET lock_configuration=true")
    peak, done = [0], threading.Event()

    def watch():
        while not done.is_set():
            peak[0] = max(peak[0], instance_memory(con) or 0)
            time.sleep(0.02)

    w = threading.Thread(target=watch)
    w.start()
    con.sql("SELECT count(*) FROM (SELECT md5(i::VARCHAR) s, count(*) "
            "FROM range(2000000) t(i) GROUP BY s)").fetchone()
    done.set()
    w.join()
    assert peak[0] > 1 << 20
    con.close()
    assert instance_memory(con) is None


def test_real_memory_interrupt_stops_the_newer_query(tmp_path):
    """Two real DuckDB queries hold memory at once; with the pod reported over
    its mark, the guard stops the one that started later and the older one runs
    to completion. The stopped query's memory counts as freeing, so the guard
    does not go on to stop the older query while the newer one unwinds."""
    duckdb = pytest.importorskip("duckdb")
    go = threading.Event()
    guard, _ = _mem_guard(MemorySample(1 * _GiB, 100 * _GiB, 400 * _GiB),
                          memory_min_victim=32 << 20)
    # From `go` on, the pod sits just over its 90% mark because of the newer
    # query, and the cgroup keeps showing that memory after the interrupt (it
    # lags the free), so only the freeing count keeps the older query safe.
    held = [0]

    def read():
        if not go.is_set():
            return MemorySample(1 * _GiB, 100 * _GiB, 400 * _GiB)
        if not guard._interrupted and "new" in conns:
            held[0] = instance_memory(conns["new"]) or held[0]
        return MemorySample(int(89.9 * _GiB) + held[0], 100 * _GiB, 400 * _GiB)
    guard._read_memory = read
    stop = threading.Event()

    def watchdog():
        while not stop.is_set():
            guard.check()
            time.sleep(0.05)

    outcome, conns = {}, {}

    def run(name, limit, rows):
        c = duckdb.connect(":memory:")
        c.sql(f"SET threads=2; SET memory_limit='{limit}'; "
              f"SET temp_directory='{tmp_path / ('duckdb-' + name)}'")
        conns[name] = c
        d = guard.register(c)
        try:
            c.sql(f"SELECT count(*) FROM (SELECT md5(i::VARCHAR) s, count(*) "
                  f"FROM range({rows}) t(i) GROUP BY s)").fetchone()
            outcome[name] = "done"
        except duckdb.InterruptException:
            outcome[name] = guard.was_interrupted(d)
        finally:
            guard.unregister(d)
            c.close()

    def wait_for(name, nbytes):
        deadline = time.time() + 30
        while time.time() < deadline and (
                instance_memory(conns[name]) if name in conns else 0) < nbytes:
            time.sleep(0.02)

    wd = threading.Thread(target=watchdog)
    wd.start()
    old = threading.Thread(target=run, args=("old", "1GB", 8_000_000))
    new = threading.Thread(target=run, args=("new", "300MB", 8_000_000))
    old.start()
    wait_for("old", 200 << 20)
    new.start()
    wait_for("new", 64 << 20)
    go.set()
    old.join()
    new.join()
    stop.set()
    wd.join()
    assert outcome == {"old": "done", "new": "memory"}
