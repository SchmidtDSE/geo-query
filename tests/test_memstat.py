"""Memory readings for the pod memory log (#519); moved from the spill guard (#551)."""
import threading
import time

import pytest

from memstat import MemorySample, instance_memory, read_memory

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
            "FROM range(2000000) t(i) GROUP BY s)").fetchall()[0]
    done.set()
    w.join()
    assert peak[0] > 1 << 20
    con.close()
    assert instance_memory(con) is None


def test_instance_memory_leaves_out_the_external_file_cache(tmp_path):
    """The file cache reserves a whole remote file as a query starts, far more
    than the query holds, so it does not count toward the query's figure (#530)."""
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "f.parquet")
    con = duckdb.connect(":memory:")
    con.sql(f"COPY (SELECT range i, md5(range::VARCHAR) s FROM range(3000000)) TO '{path}'")
    con.sql(f"SELECT count(*) FROM read_parquet('{path}') WHERE s LIKE 'a%'").fetchall()[0]
    cached = con.sql("SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory() "
                     "WHERE tag = 'EXTERNAL_FILE_CACHE'").fetchall()[0][0]
    if cached < 100_000:
        pytest.skip("this DuckDB does not cache local Parquet reads")
    # The idle connection holds little besides the cache.
    assert instance_memory(con) < cached
    con.close()
