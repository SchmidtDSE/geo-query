"""One worker process per query (#551).

These tests start real worker processes but never import `server`, so they
are safe to run anywhere: they need DuckDB, not the MCP server or S3.
"""
import os
import signal
import sys
import threading
import time

import pytest

from queryworker import (
    CANCELLED_MESSAGE,
    MEMORY_KILLED_MESSAGE,
    WorkerPool,
    isolated_db,
    render_result,
)
from querytime import TIME_LIMIT_MESSAGE

# Runs for minutes unless something stops it, and never spills.
SLOW_SQL = "SELECT sum(hash(i)) AS s FROM range(1000000000000::BIGINT) t(i)"


@pytest.fixture
def pool():
    p = WorkerPool(2, setup_sql="SET preserve_insertion_order=false")
    p.start()
    yield p
    p.close()


def _wait_for(cond, timeout=30.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); it has exited for our purposes.
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def _has_spill_files(root) -> bool:
    """True when any spill directory under root holds a file."""
    for d in os.scandir(root):
        try:
            if any(os.scandir(d.path)):
                return True
        except (FileNotFoundError, NotADirectoryError):
            pass  # removed while we looked
    return False


class TestSharedSetup:
    def test_isolated_db_runs_and_closes(self):
        with isolated_db("") as conn:
            assert conn.sql("SELECT 1").fetchone()[0] == 1
        with pytest.raises(Exception):
            conn.sql("SELECT 1")

    def test_isolated_db_is_locked_down(self):
        with isolated_db("") as conn:
            with pytest.raises(Exception):
                conn.sql("SELECT * FROM read_csv('/etc/passwd')").fetchall()

    def test_credential_pair_is_checked_before_connecting(self):
        with pytest.raises(ValueError, match="s3_secret is required"):
            with isolated_db("", s3_key="AKID"):
                pass

    def test_render_result_matches_markdown_table(self):
        with isolated_db("") as conn:
            md = render_result(conn.sql("SELECT 42 AS answer, 613762345678901234::UBIGINT AS h8"))
        assert "answer" in md and "42" in md
        assert "613762345678901234" in md  # wide ints are not rounded (#387)


class TestPool:
    def test_runs_a_query(self, pool):
        out = pool.run("SELECT 42 AS answer")
        assert "answer" in out and "42" in out

    def test_setup_sql_applies_before_lock_down(self, pool):
        out = pool.run("SELECT current_setting('preserve_insertion_order')::VARCHAR AS v")
        assert "false" in out

    def test_sql_error_comes_back_as_text(self, pool):
        out = pool.run("SELECT * FROM no_such_table")
        assert out.startswith("SQL Error:")

    def test_local_files_stay_blocked_in_workers(self, pool):
        out = pool.run("SELECT * FROM read_csv('/etc/passwd')")
        assert out.startswith("SQL Error:")

    def test_credentials_are_redacted_and_checked(self, pool):
        out = pool.run("SELECT 1", {"s3_key": "AKIDONLY1234"})
        assert out.startswith("SQL Error:") and "s3_secret is required" in out

    def test_each_worker_serves_one_query(self, pool):
        pids = set()
        for _ in range(3):
            pool.run("SELECT 1")
            pids.update(w.proc.pid for w in pool._idle)
        # Replacements keep arriving: more distinct workers than the pool size.
        assert len(pids) > pool.size
        assert pool.running_pids() == []

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="needs /proc")
    def test_workers_volunteer_for_the_oom_killer(self, pool):
        w = pool._idle[0]
        assert _wait_for(lambda: os.path.exists(f"/proc/{w.proc.pid}/oom_score_adj"))
        assert _wait_for(lambda: open(f"/proc/{w.proc.pid}/oom_score_adj").read().strip() == "1000")

    def test_time_limit_kills_the_worker(self, pool):
        started = time.monotonic()
        seen = []

        def watch():
            _wait_for(lambda: bool(pool.running_pids()))
            seen.extend(pool.running_pids())

        t = threading.Thread(target=watch)
        t.start()
        out = pool.run(SLOW_SQL, time_limit=1.0)
        t.join()
        assert out == TIME_LIMIT_MESSAGE.format(limit=1.0)
        assert time.monotonic() - started < 15
        assert seen and not _alive(seen[0])

    def test_time_limit_stops_a_geos_union(self):
        """The case behind #530: ST_Union_Agg ignores DuckDB's interrupt, so only
        killing its process stops it."""
        p = WorkerPool(1, setup_sql="LOAD spatial")
        p.start()
        try:
            sql = ("SELECT ST_Area(ST_Union_Agg(ST_Buffer(ST_Point(random()*100, random()*100), 1.5, 64))) "
                   "FROM range(400000)")
            started = time.monotonic()
            assert p.run(sql, time_limit=2.0) == TIME_LIMIT_MESSAGE.format(limit=2.0)
            assert time.monotonic() - started < 15
        finally:
            p.close()

    def test_stop_flag_kills_the_worker(self, pool):
        stop = threading.Event()
        threading.Timer(1.0, stop.set).start()
        assert pool.run(SLOW_SQL, stop=stop) == CANCELLED_MESSAGE
        assert pool.running_pids() == []

    def test_a_kill_from_outside_reads_as_out_of_memory(self, pool):
        """SIGKILL the pool did not send is what the kernel OOM killer does."""
        def oom_killer():
            assert _wait_for(lambda: bool(pool.running_pids()))
            os.kill(pool.running_pids()[0], signal.SIGKILL)

        t = threading.Thread(target=oom_killer)
        t.start()
        out = pool.run(SLOW_SQL)
        t.join()
        assert out == MEMORY_KILLED_MESSAGE

    def test_killed_worker_spill_directory_is_removed(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
        # A tiny memory limit makes the query below spill; workers inherit it.
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "64MB")
        p = WorkerPool(1, setup_sql="")
        p.start()
        try:
            # A large DISTINCT spills its hash table within a second at 64 MB.
            sql = "SELECT count(DISTINCT md5(i::VARCHAR)) FROM range(20000000) t(i)"
            stop = threading.Event()
            spilled = []

            def stop_when_spilling():
                spilled.append(_wait_for(lambda: _has_spill_files(tmp_path), timeout=60))
                stop.set()

            t = threading.Thread(target=stop_when_spilling)
            t.start()
            p.run(sql, stop=stop)
            stop.set()
            t.join()
            assert spilled == [True], "the query never spilled, so this test checks nothing"
            # Only the idle replacement's directory may remain, and it is empty
            # because DuckDB creates its files on first spill.
            assert not _has_spill_files(tmp_path)
        finally:
            p.close()

    def test_close_stops_idle_workers(self):
        p = WorkerPool(2, setup_sql="")
        p.start()
        pids = [w.proc.pid for w in p._idle]
        p.close()
        assert not any(_alive(pid) for pid in pids)
        with pytest.raises(RuntimeError):
            p.run("SELECT 1")
