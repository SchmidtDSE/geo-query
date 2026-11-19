"""Unit tests for dbconfig — the DuckDB memory_limit (#270) and spill (#423) derivation."""
import os
import subprocess
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dbconfig import (
    _parse_bytes,
    connect_config,
    duckdb_max_temp_directory_size,
    duckdb_memory_limit,
    memory_limit_sql,
    memory_slots,
    query_threads,
    spill_sql,
)


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    """Each test starts with no memory/spill var set."""
    for var in ("DUCKDB_MEMORY_LIMIT", "POD_MEMORY_LIMIT", "DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
                "POD_EPHEMERAL_LIMIT", "DUCKDB_TEMP_ROOT", "DUCKDB_MEMORY_SLOTS",
                "MCP_QUERY_CONCURRENCY", "TILE_BUILD_MAX_CONCURRENCY", "QUERY_THREADS"):
        monkeypatch.delenv(var, raising=False)


class TestParseBytes:
    def test_plain_integer_is_bytes(self):
        # The Downward API emits the limit in bytes as a plain integer.
        assert _parse_bytes("104857600") == 100 * 2**20

    @pytest.mark.parametrize("text,expected", [
        ("96Gi", 96 * 2**30),
        ("2Gi", 2 * 2**30),
        ("500Mi", 500 * 2**20),
        ("10G", 10 * 10**9),
        ("1Ti", 2**40),
    ])
    def test_k8s_and_si_quantities(self, text, expected):
        assert _parse_bytes(text) == expected

    @pytest.mark.parametrize("bad", ["", "abc", "10Xi", "Gi"])
    def test_unparseable_raises(self, bad):
        with pytest.raises(ValueError):
            _parse_bytes(bad)


class TestDuckdbMemoryLimit:
    def test_unset_returns_none(self):
        assert duckdb_memory_limit() is None
        assert memory_limit_sql() == ""

    def test_explicit_override_passthrough(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "120GB")
        assert duckdb_memory_limit() == "120GB"
        assert memory_limit_sql() == "SET memory_limit='120GB'"

    def test_explicit_override_wins_over_pod_limit(self, monkeypatch):
        monkeypatch.setenv("POD_MEMORY_LIMIT", str(96 * 2**30))
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "100GB")
        assert duckdb_memory_limit() == "100GB"

    def test_pod_limit_bytes_is_80_percent_in_mib_with_one_slot(self, monkeypatch):
        # 100 MiB → 80% = 80 MiB, all of it for the one connection.
        monkeypatch.setenv("DUCKDB_MEMORY_SLOTS", "1")
        monkeypatch.setenv("POD_MEMORY_LIMIT", str(100 * 2**20))
        assert duckdb_memory_limit() == "80MiB"
        assert memory_limit_sql() == "SET memory_limit='80MiB'"

    def test_pod_limit_split_across_slots(self, monkeypatch):
        # The production shape (#519): 160 GiB pod, 8 queries + 2 builds + the
        # tile connection = 11 slots, so 11 connections at their limit stay
        # within 80% of the pod rather than claiming ~1 TiB.
        monkeypatch.setenv("POD_MEMORY_LIMIT", "160Gi")
        expected_mib = int(160 * 2**30 * 0.8 / 11) // 2**20
        assert duckdb_memory_limit() == f"{expected_mib}MiB"
        assert 11 * expected_mib * 2**20 <= 160 * 2**30 * 0.8

    def test_explicit_override_is_per_connection(self, monkeypatch):
        monkeypatch.setenv("POD_MEMORY_LIMIT", "160Gi")
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "16GiB")
        assert duckdb_memory_limit() == "16GiB"

    def test_unparseable_pod_limit_falls_back_to_none(self, monkeypatch, capsys):
        monkeypatch.setenv("POD_MEMORY_LIMIT", "not-a-size")
        assert duckdb_memory_limit() is None
        assert "POD_MEMORY_LIMIT ignored" in capsys.readouterr().err

    def test_zero_pod_limit_returns_none(self, monkeypatch):
        monkeypatch.setenv("POD_MEMORY_LIMIT", "0")
        assert duckdb_memory_limit() is None


class TestMemorySlots:
    def test_default_is_queries_plus_builds_plus_tile_connection(self):
        assert memory_slots() == 8 + 2 + 1

    def test_follows_concurrency_env(self, monkeypatch):
        monkeypatch.setenv("MCP_QUERY_CONCURRENCY", "4")
        monkeypatch.setenv("TILE_BUILD_MAX_CONCURRENCY", "1")
        assert memory_slots() == 6

    def test_explicit_override(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MEMORY_SLOTS", "3")
        assert memory_slots() == 3

    def test_unparseable_falls_back(self, monkeypatch, capsys):
        monkeypatch.setenv("MCP_QUERY_CONCURRENCY", "eight")
        assert memory_slots() == 11
        assert "unparseable" in capsys.readouterr().err


class TestConnectConfig:
    def test_pinning_off_and_background_release(self):
        import duckdb
        con = duckdb.connect(":memory:", config=connect_config(3))
        settings = dict(con.sql(
            "SELECT name, value FROM duckdb_settings() WHERE name IN "
            "('pin_threads', 'allocator_background_threads', 'threads')").fetchall())
        con.close()
        assert settings == {"pin_threads": "off", "allocator_background_threads": "true",
                            "threads": "3"}

    def test_threads_omitted_by_default(self):
        assert "threads" not in connect_config()

    def test_query_threads_env(self, monkeypatch):
        assert query_threads() is None
        monkeypatch.setenv("QUERY_THREADS", "16")
        assert query_threads() == 16
        monkeypatch.setenv("QUERY_THREADS", "lots")
        assert query_threads() is None


class TestMaxTempDirectorySize:
    def test_unset_returns_none(self):
        assert duckdb_max_temp_directory_size() is None

    def test_explicit_override_wins(self, monkeypatch):
        monkeypatch.setenv("POD_EPHEMERAL_LIMIT", "50Gi")
        monkeypatch.setenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "10GiB")
        assert duckdb_max_temp_directory_size() == "10GiB"

    def test_pod_limit_is_seventy_percent_in_mib(self, monkeypatch):
        # The NRP namespace default: 50Gi → 35 GiB per connection, a backstop
        # under spillguard's pod-wide limit (#428).
        monkeypatch.setenv("POD_EPHEMERAL_LIMIT", str(50 * 2**30))
        assert duckdb_max_temp_directory_size() == f"{35 * 1024}MiB"

    def test_unparseable_falls_back_to_none(self, monkeypatch, capsys):
        monkeypatch.setenv("POD_EPHEMERAL_LIMIT", "lots")
        assert duckdb_max_temp_directory_size() is None
        assert "POD_EPHEMERAL_LIMIT ignored" in capsys.readouterr().err


class TestSpillSql:
    def test_private_directory_per_connection(self):
        a, b = spill_sql(), spill_sql()
        assert len(a) == 1  # no cap when the pod limit is unknown
        assert a[0].startswith("SET temp_directory='/tmp/duckdb-")
        assert a[0] != b[0]

    def test_temp_root_and_cap(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_TEMP_ROOT", "/scratch")
        monkeypatch.setenv("POD_EPHEMERAL_LIMIT", "2Gi")
        tmp, cap = spill_sql()
        assert tmp.startswith("SET temp_directory='/scratch/duckdb-")
        assert cap == "SET max_temp_directory_size='1433MiB'"


# Real DuckDB, tiny limits: a sort of ~10M md5 strings under a 64MB memory_limit
# has to spill a few hundred MB.
_SPILL_QUERY = ("SELECT sum(hash(s)) FROM (SELECT md5(i::VARCHAR) s "
                "FROM range(4000000) t(i) ORDER BY s)")


def _spill_conn(duckdb):
    con = duckdb.connect(":memory:")
    con.sql("SET threads=2; SET memory_limit='64MB'; SET preserve_insertion_order=false")
    for stmt in spill_sql():
        con.sql(stmt)
    return con


class TestSpillBehaviour:
    def test_cap_fails_the_query_not_the_process(self, monkeypatch, tmp_path):
        duckdb = pytest.importorskip("duckdb")
        monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
        monkeypatch.setenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "32MiB")
        con = _spill_conn(duckdb)
        with pytest.raises(duckdb.OutOfMemoryException, match="max_temp_directory_size"):
            con.sql(_SPILL_QUERY).fetchall()
        con.close()
        assert list(tmp_path.iterdir()) == []  # DuckDB removed its spill dir

    def test_concurrent_spills_do_not_collide(self, tmp_path):
        # Shared '/tmp' segfaulted here every time (#423); run in a subprocess so a
        # regression reports as a failure rather than killing the test runner.
        pytest.importorskip("duckdb")
        code = textwrap.dedent(f"""
            import sys, threading
            sys.path.insert(0, {os.path.join(os.path.dirname(__file__), "..")!r})
            import duckdb
            from tests.test_dbconfig import _SPILL_QUERY, _spill_conn
            out = []
            def run():
                con = _spill_conn(duckdb)
                out.append(con.sql(_SPILL_QUERY).fetchall())
                con.close()
            ts = [threading.Thread(target=run) for _ in range(3)]
            [t.start() for t in ts]; [t.join() for t in ts]
            assert len(out) == 3 and out[0] == out[1] == out[2], out
        """)
        env = {**os.environ, "DUCKDB_TEMP_ROOT": str(tmp_path)}
        r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                           text=True, timeout=300)
        assert r.returncode == 0, f"exit {r.returncode}: {r.stderr[-2000:]}"
        assert list(tmp_path.iterdir()) == []


# --- Filesystem lockdown (NRP incident 2026-11-06) --------------------------
import duckdb as _duckdb
import pytest as _pytest

from dbconfig import (
    disable_persistent_secrets_sql,
    lock_down,
    spill_sql,
    locked_hint,
)


def _locked_conn(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
    con = _duckdb.connect(":memory:")
    con.sql(disable_persistent_secrets_sql())
    for stmt in spill_sql():
        con.sql(stmt)
    lock_down(con)
    return con


@_pytest.mark.parametrize("sql", [
    "SELECT * FROM read_text('/etc/hostname')",
    "SELECT * FROM read_blob('/proc/self/environ')",
    "SELECT * FROM glob('/var/run/*')",
    "COPY (SELECT 1) TO '/tmp/lockdown-probe.csv'",
    "ATTACH '/tmp/lockdown-probe.db'",
    "SELECT * FROM read_text('http://example.com/')",
    "SET enable_external_access=true",
    "SET allowed_directories=['/']",
    "SET temp_directory='/tmp'",
    "SET memory_limit='100GB'",
    "SET threads=4",
    "RESET memory_limit",
    "PRAGMA threads=4",
    "SET lock_configuration=false",
    "INSTALL fts",
])
def test_lock_down_refuses_local_access(tmp_path, monkeypatch, sql):
    con = _locked_conn(tmp_path, monkeypatch)
    with _pytest.raises(_duckdb.Error):
        con.sql(sql).fetchall()


def test_lock_down_still_spills(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
    con = _duckdb.connect(":memory:")
    con.sql(disable_persistent_secrets_sql())
    con.sql("SET memory_limit='200MB'")
    con.sql("SET threads=2")
    con.sql("SET preserve_insertion_order=false")
    for stmt in spill_sql():
        con.sql(stmt)
    lock_down(con)
    n = con.sql("SELECT count(*) FROM (SELECT i % 3000000 AS k, sum(i) FROM range(20000000) t(i) GROUP BY k)").fetchone()[0]
    assert n == 3000000


def test_lock_down_allows_extra_local_dir(tmp_path, monkeypatch):
    out = tmp_path / "tiles"
    out.mkdir()
    monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
    con = _duckdb.connect(":memory:")
    for stmt in spill_sql():
        con.sql(stmt)
    lock_down(con, extra_dirs=(str(out), "s3://public-output"))
    con.sql(f"COPY (SELECT 1 AS x) TO '{out}/a.csv'")
    assert (out / "a.csv").exists()


def test_lock_down_keeps_loaded_extensions_usable(tmp_path, monkeypatch):
    """LOAD of an already-loaded extension and its functions still work."""
    monkeypatch.setenv("DUCKDB_TEMP_ROOT", str(tmp_path))
    con = _duckdb.connect(":memory:")
    con.sql(disable_persistent_secrets_sql())
    con.sql("LOAD h3")
    for stmt in spill_sql():
        con.sql(stmt)
    lock_down(con)
    con.sql("LOAD h3")
    assert con.sql("SELECT h3_latlng_to_cell(37.8, -122.3, 6)").fetchone()[0] > 0
    assert con.sql("SELECT current_setting('lock_configuration')").fetchone()[0] is True


def test_locked_hint_only_for_locked_setup_errors():
    assert locked_hint('Invalid Input Error: Cannot change configuration option "threads" - the configuration has been locked')
    assert locked_hint('Permission Error: Cannot access directory "/home/app/.duckdb/extensions/v1.5.4" - file system operations are disabled by configuration')
    assert not locked_hint('Permission Error: Cannot access file "/etc/hostname" - file system operations are disabled by configuration')
    assert not locked_hint("Catalog Error: Table with name foo does not exist!")
