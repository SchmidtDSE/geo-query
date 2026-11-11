"""Persistent :memory: DuckDB connection for the tile endpoint.

Separate from the per-request isolated connections used by the `query` tool:
tile requests never take user credentials, so the connection can be long-lived
and shared across requests via con.cursor() for per-request isolation.
"""
import os
import sys

import duckdb

from s3config import default_endpoint, default_s3_secret_sql, infer_use_ssl, source_secret_sql, sql_quote
from dbconfig import disable_persistent_secrets_sql, lock_down, memory_limit_sql, spill_sql


def tile_bucket_base() -> str:
    return os.environ.get("TILE_BUCKET_BASE", "s3://public-output")


def split_tile_writes() -> bool:
    """True when a dedicated tile write key is configured (TILE_WRITE_KEY_ID).

    Then caller SQL never runs on a connection that can write the tile bucket:
    connections built by build_tile_connection() hold read credentials only, and
    every write to TILE_BUCKET_BASE goes through build_tile_writer_connection(),
    which never runs caller SQL. Private replicas that write tiles to a private
    bucket set it. Unset, the default: one kind of connection does both, as on
    the public deployments, whose tiles go anonymously to s3://public-output.
    """
    return bool(os.environ.get("TILE_WRITE_KEY_ID", "").strip())


def tile_write_secret_sql() -> str:
    """CREATE SECRET for the tile write key, scoped to TILE_BUCKET_BASE.

    TILE_WRITE_KEY_ID / TILE_WRITE_SECRET hold the key. The endpoint is
    TILE_WRITE_ENDPOINT (bare host), defaulting to the deployment default;
    TILE_WRITE_USE_SSL overrides the inferred SSL setting. The key needs list,
    read and write on the tile bucket: builds read back each level they write.
    """
    endpoint = os.environ.get("TILE_WRITE_ENDPOINT", "").strip() or default_endpoint()
    use_ssl = (os.environ.get("TILE_WRITE_USE_SSL") or infer_use_ssl(endpoint)).strip().lower()
    url_style = os.environ.get("TILE_WRITE_URL_STYLE", "path")
    return (
        "CREATE OR REPLACE SECRET tile_write ("
        f"TYPE S3, KEY_ID '{sql_quote(os.environ.get('TILE_WRITE_KEY_ID', '').strip())}', "
        f"SECRET '{sql_quote(os.environ.get('TILE_WRITE_SECRET', '').strip())}', "
        f"ENDPOINT '{sql_quote(endpoint)}', URL_STYLE '{sql_quote(url_style)}', "
        f"USE_SSL '{sql_quote(use_ssl)}', SCOPE '{sql_quote(tile_bucket_base())}')"
    )


def _base_tile_connection(threads: int | None) -> duckdb.DuckDBPyConnection:
    """Create a :memory: connection with extensions loaded and tuned, no secrets yet.

    Extensions are pre-installed in the image (Dockerfile INSTALL step); LOAD is
    per-session. Dev environments must run the Dockerfile or manually INSTALL once.

    `threads` sets DuckDB's per-query parallelism for this connection:
    - The shared READ connection (tile-serve GETs + prepare-phase probes) uses
      the default (TILE_THREADS, 48). Harmless there — those queries are tiny
      (LIMIT 0 / LIMIT 1 / single-tile reads).
    - Pyramid BUILD connections pass server._BUILD_THREADS (env TILE_BUILD_THREADS,
      default 48 — same as reads, leaving ~16 cores free under the 64-core limit).
      A build pegs every thread for minutes; the headroom keeps the uvicorn event
      loop schedulable so /healthz answers (#184/#185). Drop it via env only on
      under-provisioned nodes. Builds run on their own connection
      (server._build_executor) so this never slows tile reads.
    """
    con = duckdb.connect(":memory:")
    con.sql(disable_persistent_secrets_sql())
    con.sql("LOAD httpfs")
    con.sql("LOAD spatial")
    con.sql("LOAD h3")

    if threads is None:
        threads = int(os.environ.get("TILE_THREADS", "48"))
    con.sql(f"SET THREADS={int(threads)}")
    con.sql("SET preserve_insertion_order=false")
    con.sql("SET enable_object_cache=true")

    # Bound memory to the pod so a heavy pyramid build spills instead of OOM-killing
    # the replica (#270); no-op unless POD_MEMORY_LIMIT/DUCKDB_MEMORY_LIMIT is set.
    # Guarded because the value can come from an operator override (DUCKDB_MEMORY_LIMIT)
    # — a malformed one shouldn't take down tile serving, just fall back to the default.
    mem_sql = memory_limit_sql()
    if mem_sql:
        try:
            con.sql(mem_sql)
        except Exception as e:
            print(f"⚠️ tile memory_limit setup skipped: {mem_sql!r}: {e}", file=sys.stderr)

    # Private, bounded spill directory (#423): pyramid builds run concurrently with
    # each other and with query connections, and DuckDB instances sharing one
    # temp_directory overwrite each other's spill files and segfault the process.
    for stmt in spill_sql():
        try:
            con.sql(stmt)
        except Exception as e:
            print(f"⚠️ tile spill setup skipped: {stmt!r}: {e}", file=sys.stderr)

    # httpfs read tuning. NOTE: the "~126 files per h0 / ~923 files" figure behind
    # the original #190 characterization was a since-fixed GBIF over-sharding bug,
    # NOT steady state. Every queryable hex dataset is now 1 file per h0 (carbon:
    # 122 files @112 MB; gbif 2026: 122 @3 GB; padus: 21 @356 MB), so a pruned query
    # opens ONE footer and a global scan opens <=122 — per-file-open latency is no
    # longer the dominant cost; large scans are bandwidth-bound on the byte stream.
    # metadata + connection caching still help the long-lived tile-serve connection
    # that re-reads the same files across tile requests, so they stay.
    con.sql("SET enable_http_metadata_cache=true")
    con.sql("SET httpfs_connection_caching=true")
    # prefetch_all_parquet_files: no-op for a 1-file pruned read; its measured
    # ~17-26% gain came entirely from the over-sharded case. Kept pending a
    # re-measure at 1-file/h0 (see benchmarks/s3-throughput-bench.py); drop if neutral.
    con.sql("SET prefetch_all_parquet_files=true")
    return con


def build_tile_connection(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """A connection that may run caller SQL: tile serving, the prepare-phase
    probes and the source side of a pyramid build.

    It holds the deployment's read credentials (the default secret and the
    registry sources). With split_tile_writes() it never holds the tile write
    key; otherwise it also writes the tiles, anonymously or with whatever the
    registry grants, as it always has.
    """
    con = _base_tile_connection(threads)
    # S3 access: the deployment default endpoint (S3_DEFAULT_ENDPOINT, same env
    # surface as the query tool — #268/#271). Previously hardcoded to the
    # cluster-internal Ceph endpoint, which broke hex tiles (source reads AND
    # s3://public-output writes) on any deployment repointed via env.
    con.sql(default_s3_secret_sql())
    # Scoped registry-source secrets (#264), same set as the query tool — so a
    # register_hex_tiles SQL over e.g. source.coop mirror paths routes correctly
    # here too (previously only the query connections had the mirror secret).
    for stmt in source_secret_sql():
        con.sql(stmt)
    # Last: register_hex_tiles runs caller SQL here too, so no local file access
    # (NRP incident 2026-11-06). A local TILE_BUCKET_BASE (tests) stays reachable
    # so tiles can be served from it. On S3, what this connection may write is
    # decided by its credentials: with split_tile_writes() it has no write key.
    lock_down(con, extra_dirs=(tile_bucket_base(),))
    return con


def build_tile_writer_connection(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """A connection for writing the tile bucket. It must never run caller SQL.

    Without split_tile_writes() this is build_tile_connection(), so public
    deployments keep one kind of connection. With it, the connection holds only
    the tile write key, scoped to TILE_BUCKET_BASE: it writes the pyramid
    (streamed from a source connection), reads back the levels it wrote, and
    writes metadata and the lock and failure markers.
    """
    if not split_tile_writes():
        return build_tile_connection(threads)
    con = _base_tile_connection(threads)
    con.sql(tile_write_secret_sql())
    lock_down(con, extra_dirs=(tile_bucket_base(),))
    return con
