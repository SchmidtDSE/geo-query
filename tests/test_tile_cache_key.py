"""The tile cache key covers mutable source files, not just the SQL text (#571)."""
import os
import time

import duckdb
import pytest

from tiles.pyramid import mutable_source_paths, source_fingerprint
from tiles.tile_math import content_hash


@pytest.fixture
def bucket(tmp_path, monkeypatch):
    """A local stand-in for the tile bucket, which always counts as mutable."""
    monkeypatch.setenv("TILE_BUCKET_BASE", str(tmp_path))
    return tmp_path


def _write(con, path, n):
    con.sql(f"COPY (SELECT range AS h, range * 2 AS v FROM range({n})) TO '{path}' (FORMAT parquet)")


class TestMutableSourcePaths:
    def test_public_output_in_s3_and_https_form(self):
        sql = ("SELECT * FROM read_parquet('s3://public-output/a.parquet') "
               "JOIN read_parquet('https://s3-west.nrp-nautilus.io/public-output/b/*.parquet') USING (h)")
        assert mutable_source_paths(sql) == [
            "https://s3-west.nrp-nautilus.io/public-output/b/*.parquet",
            "s3://public-output/a.parquet",
        ]

    def test_catalog_paths_are_not_mutable(self):
        sql = "SELECT h8, carbon FROM read_parquet('s3://public-carbon/irrecoverable-carbon-2024/hex/h0=*/data_0.parquet')"
        assert mutable_source_paths(sql) == []

    def test_a_bucket_name_inside_another_path_does_not_match(self):
        sql = "SELECT * FROM read_parquet('s3://public-data/public-output/x.parquet')"
        assert mutable_source_paths(sql) == []

    def test_buckets_are_configurable(self, monkeypatch):
        monkeypatch.setenv("TILE_BUCKET_BASE", "s3://private-tiles")
        monkeypatch.setenv("TILE_CACHE_MUTABLE_BUCKETS", "scratch, other/")
        sql = "SELECT 1 FROM read_parquet('s3://scratch/x.parquet'), read_parquet('s3://public-output/y.parquet')"
        assert mutable_source_paths(sql) == ["s3://scratch/x.parquet"]

    def test_the_tile_bucket_always_counts(self, bucket):
        sql = f"SELECT * FROM read_parquet('{bucket}/x.parquet')"
        assert mutable_source_paths(sql) == [f"{bucket}/x.parquet"]


class TestSourceFingerprint:
    def test_empty_without_mutable_sources(self):
        assert source_fingerprint(duckdb.connect(), "SELECT 1 AS h, 2 AS v") == ""

    def test_stable_while_the_file_is_unchanged(self, bucket):
        con = duckdb.connect()
        _write(con, bucket / "a.parquet", 10)
        sql = f"SELECT h, v FROM read_parquet('{bucket}/a.parquet')"
        assert source_fingerprint(con, sql) == source_fingerprint(con, sql) != ""

    def test_changes_when_the_file_is_rewritten(self, bucket):
        """The benchmark case: two sessions write different data to the same
        path, then register the same SQL. The second must not get the first's tiles."""
        con = duckdb.connect()
        path = bucket / "a.parquet"
        sql = f"SELECT h, v FROM read_parquet('{path}')"
        _write(con, path, 10)
        before = source_fingerprint(con, sql)
        time.sleep(0.01)
        _write(con, path, 10)  # same size; a later modified time
        os.utime(path, None)
        assert source_fingerprint(con, sql) != before

    def test_covers_every_file_a_glob_matches(self, bucket):
        con = duckdb.connect()
        (bucket / "d").mkdir()
        _write(con, bucket / "d" / "1.parquet", 5)
        sql = f"SELECT h, v FROM read_parquet('{bucket}/d/*.parquet')"
        before = source_fingerprint(con, sql)
        _write(con, bucket / "d" / "2.parquet", 5)
        assert source_fingerprint(con, sql) != before

    def test_unlistable_path_gets_a_one_off_key(self, bucket):
        con = duckdb.connect()
        sql = f"SELECT h, v FROM read_parquet('{bucket}/missing.parquet')"
        a, b = source_fingerprint(con, sql), source_fingerprint(con, sql)
        assert a.startswith("uncached:") and a != b


class TestContentHash:
    def test_hash_without_sources_is_unchanged(self):
        """SQL over immutable data keeps its existing cache key and tile URLs."""
        args = dict(sql="SELECT 1", finest_res=8, min_res=2, agg="AVG", zoom_offset=4)
        assert content_hash(**args) == content_hash(**args, sources="")

    def test_sources_change_the_hash(self):
        args = dict(sql="SELECT 1", finest_res=8, min_res=2, agg="AVG", zoom_offset=4)
        assert content_hash(**args, sources="abc") != content_hash(**args)
