"""Pod-wide spill guard (#428)."""
import os
import threading
import time

import pytest

from spillguard import SpillGuard
from tests.test_dbconfig import _SPILL_QUERY, _spill_conn


def _fill(path, nbytes):
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "duckdb_temp_storage-0.tmp"), "wb") as f:
        f.write(os.urandom(nbytes))


def test_off_without_a_limit(tmp_path):
    guard = SpillGuard(limit=None, root=str(tmp_path))
    assert not guard.enabled
    guard.check()
    assert not guard.under_pressure()
    assert guard.start() is None


def test_usage_counts_only_duckdb_spill_dirs(tmp_path):
    _fill(tmp_path / "duckdb-a", 1 << 20)
    _fill(tmp_path / "other", 1 << 20)
    guard = SpillGuard(limit=10 << 20, root=str(tmp_path))
    usage = guard.usage()
    assert list(usage) == [os.path.normpath(tmp_path / "duckdb-a")]
    assert usage[os.path.normpath(tmp_path / "duckdb-a")] >= 1 << 20


def test_pressure_turns_on_at_the_drain_mark_and_off_below_it(tmp_path):
    guard = SpillGuard(limit=10 << 20, root=str(tmp_path),
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


def test_interrupts_the_largest_registered_spill_once(tmp_path):
    guard = SpillGuard(limit=10 << 20, root=str(tmp_path),
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
    guard = SpillGuard(limit=10 << 20, root=str(tmp_path), interrupt_fraction=0.5)
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
    probe = SpillGuard(limit=1 << 40, root=str(tmp_path))
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

    guard = SpillGuard(limit=int(peak[0] * 1.6), root=str(tmp_path),
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
    guard = SpillGuard(limit=50 << 30, root=str(tmp_path))
    assert guard.drain_at == 25 << 30
    assert guard.interrupt_at == 40 << 30
