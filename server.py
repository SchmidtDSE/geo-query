from auth import CALLER, caller_label, load_tokens, match_token
from memstat import read_memory as _read_memory, instance_memory as _instance_memory
import os
import re
import socket
import threading
import time
import duckdb
import uvicorn
import sys
import anyio
from contextlib import contextmanager
from mcp.server.fastmcp import FastMCP
from rebinding import transport_security_from_env, describe as _describe_transport_security
from mcp.shared.session import BaseSession
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# Workaround for https://github.com/SchmidtDSE/geo-query/issues/5
# send_notification crashes with ClosedResourceError when the client disconnects
# (e.g. after a ~60s client-side timeout) while a query is still running.
# The MCP library should catch this in send_notification; patch it until upstream fixes it.
_orig_send_notification = BaseSession.send_notification
async def _resilient_send_notification(self, notification, related_request_id=None):
    try:
        await _orig_send_notification(self, notification, related_request_id)
    except anyio.ClosedResourceError:
        pass
BaseSession.send_notification = _resilient_send_notification

# -------------------------------------------------------------------------
# 1. INITIALIZATION
# -------------------------------------------------------------------------
# App version + git SHA, baked at build time from the git tag (Dockerfile ARGs set
# by docker.yml). The tag is the single source of truth — no version string lives in
# source, so nothing can drift from what was actually tagged/built. Defaults mark a
# local/un-stamped run. See issue #221.
APP_VERSION = os.environ.get("APP_VERSION", "dev")
GIT_SHA = os.environ.get("GIT_SHA", "unknown")

# DNS-rebinding protection is off unless the deployment lists its hosts (#435):
# public deployments are reached from any origin, under hostnames the ingress
# picks; a server on localhost should set MCP_ALLOWED_HOSTS/ORIGINS.
try:
    _TRANSPORT_SECURITY = transport_security_from_env()
except ValueError as e:
    print(f"💀 {e}", file=sys.stderr)
    sys.exit(1)

mcp = FastMCP(
    "DuckDB-S3-Geo-Isolated",
    stateless_http=True,
    transport_security=_TRANSPORT_SECURITY,
)

# Report APP_VERSION in the MCP `initialize` handshake (serverInfo.version) so clients
# learn it in-band, no extra request. FastMCP has no version kwarg, so set it on the
# wrapped low-level server. `mcp` is unpinned (the weekly cron re-resolves it), so guard
# against a future internal rename rather than crash startup — /version and /healthz
# still carry the version regardless.
try:
    mcp._mcp_server.version = APP_VERSION
except Exception:
    pass

# -------------------------------------------------------------------------
# 2. CONFIGURATION & FILE LOADING
# -------------------------------------------------------------------------
def load_text_file(filename):
    paths = [
        filename,
        os.path.join("/app", filename),
        os.path.join(os.path.dirname(__file__), filename)
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f: return f.read()
    print(f"⚠️ Warning: Could not find {filename}", file=sys.stderr)
    return ""

def parse_setup_sql(content):
    match = re.search(r"```sql\n(.*?)\n```", content, re.DOTALL)
    return match.group(1).strip() if match else ""

# `<!-- prov: ... -->` lines under each guidance heading record why a rule exists:
# motivating issue, the model(s) that actually failed, when it was added, and the
# geo-agent-benchmark cell that regresses if it is removed (#384). They live next to
# the rule so they cannot drift from it, and are stripped here so they cost the model
# nothing — every byte of these files is injected into the `query` tool description on
# every call. Kept out of the docs/ tree for the same reason: adjacency is the point.
PROV_RE = re.compile(r"^[ \t]*<!--[ \t]*prov:.*?-->[ \t]*\n", re.MULTILINE)

def strip_prov(text):
    return PROV_RE.sub("", text)

# `tier=extra` marks guidance that only weak models need — a rule whose absence costs
# turns rather than correctness, and whose motivating failure was last observed on a model
# outside the shipping set (#384). Those sections are dropped unless EXTRA_INSTRUCTIONS is
# set, so a deployment serving small open models can opt back in without forking the file.
#
# Single source of truth on purpose: the tier is one word in the section's own `prov` line,
# adjacent to the rule it governs. A parallel weak-model-guide.md would drift within two
# cycles, and a one-word change in a diff is reviewable.
#
# A section runs from its heading to the next heading at the SAME OR HIGHER level, so
# demoting a `##` takes its `###` children with it. A section with no `prov` line, or none
# naming a tier, is treated as `core` — the safe default, since guidance is load-bearing
# until shown otherwise. CI (#384 stage 4) is what requires the line to exist at all.
_HEADING_RE = re.compile(r"^(#{2,6})[ \t]+\S")
_TIER_RE = re.compile(r"\btier=([A-Za-z0-9_-]+)")

def _section_tier(lines, i):
    """Tier declared by the `prov` line belonging to the heading at `lines[i]`, or None.

    Only the run of lines between the heading and the first non-blank, non-prov line is
    searched, so a `tier=` mentioned in prose further down cannot be picked up.
    """
    for line in lines[i + 1:]:
        if not line.strip():
            continue
        m = PROV_RE.match(line if line.endswith("\n") else line + "\n")
        if not m:
            return None
        t = _TIER_RE.search(line)
        return t.group(1).lower() if t else None
    return None

def _heading_text(line):
    return line.lstrip("#").strip()


def select_tiers(text, include_extra, drop=(), dropped=None):
    """Drop `tier=extra` sections unless `include_extra`, and any section whose
    heading starts with an entry of `drop` (case-insensitive; see GUIDANCE_DROP).
    Headings dropped for `drop` are appended to `dropped` when given. Prov lines
    are stripped either way."""
    drop = [d.lower() for d in drop if d]
    lines = text.splitlines(keepends=True)
    keep, i = [], 0
    while i < len(lines):
        m = _HEADING_RE.match(lines[i])
        if m:
            heading = _heading_text(lines[i])
            ablated = any(heading.lower().startswith(d) for d in drop)
            if ablated or (not include_extra and _section_tier(lines, i) == "extra"):
                if ablated and dropped is not None:
                    dropped.append(heading)
                level = len(m.group(1))
                j = i + 1
                while j < len(lines):
                    m2 = _HEADING_RE.match(lines[j])
                    if m2 and len(m2.group(1)) <= level:
                        break
                    j += 1
                i = j
                continue
        keep.append(lines[i])
        i += 1
    return strip_prov("".join(keep))

# Off by default: `duckdb-mcp` and `dev-duckdb-mcp` serve core only, which is what the
# `standard`/shipping model sets are gated against.
EXTRA_INSTRUCTIONS = os.environ.get("EXTRA_INSTRUCTIONS", "").strip().lower() in (
    "1", "true", "yes", "on")

SETUP_RAW = load_text_file("query-setup.md")
SETUP_SQL = parse_setup_sql(SETUP_RAW)
# Guidance ablation (#522 audit): GUIDANCE_DROP names sections to leave out of the
# `query` description, as heading prefixes separated by ";" (headings contain commas),
# e.g. "Problem 3 — Raster pixels;Area Conversion". For the benchmark server only, to
# measure whether a section still changes answers; /version reports what was dropped
# so every transcript records it. An entry that matches no heading is a typo that
# would silently measure nothing, so startup fails on it.
GUIDANCE_DROP = [d.strip() for d in os.environ.get("GUIDANCE_DROP", "").split(";") if d.strip()]
GUIDANCE_DROPPED: list[str] = []
OPTIM_RAW = select_tiers(load_text_file("query-optimization.md"), EXTRA_INSTRUCTIONS,
                         GUIDANCE_DROP, GUIDANCE_DROPPED)
H3_RAW = select_tiers(load_text_file("h3-guide.md"), EXTRA_INSTRUCTIONS,
                      GUIDANCE_DROP, GUIDANCE_DROPPED)
_unmatched = [d for d in GUIDANCE_DROP
              if not any(h.lower().startswith(d.lower()) for h in GUIDANCE_DROPPED)]
if _unmatched:
    raise SystemExit(f"GUIDANCE_DROP entries match no guidance heading: {_unmatched}")
if GUIDANCE_DROPPED:
    print(f"✂️ guidance sections dropped (GUIDANCE_DROP): {GUIDANCE_DROPPED}", file=sys.stderr)
ROLE_RAW = load_text_file("assistant-role.md")

# Opt-in DuckDB extensions beyond the stock httpfs/spatial/h3 set (issue #354).
# The experimental image (Dockerfile.experimental) installs raster + zarr and sets
# EXTRA_DUCKDB_EXTENSIONS; the default image leaves it unset and behaves exactly as
# before. LOAD is per-connection, like every statement in SETUP_SQL, so this is a
# deployment knob rather than a code fork. Names are restricted to identifier
# characters — they are interpolated into LOAD, which takes no parameters.
_EXT_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
EXTRA_EXTENSIONS = [
    e for e in (s.strip() for s in os.environ.get("EXTRA_DUCKDB_EXTENSIONS", "").split(","))
    if e and _EXT_NAME_RE.match(e)
]
# Guidance for the extras is injected only where they are actually loaded, so the
# default deployment's tool description is unchanged.
EXTRAS_RAW = load_text_file("experimental-extensions.md") if EXTRA_EXTENSIONS else ""

# -------------------------------------------------------------------------
# 3. CONTEXT INJECTION (PROMPT ENGINEERING)
# -------------------------------------------------------------------------
# Dataset discovery belongs to the client's catalog tools, not this server
# (#528). glen answers `get_schema` and `get_stac_details` in the browser, so the
# guidance names those as examples without promising the server provides them.
_QUERY_DISCOVER_STEPS = """1. Get the dataset's read_parquet() paths and column names from its STAC metadata (your catalog tool, such as `get_schema` or `get_stac_details`).
2. Use ONLY those paths — never guess or hardcode any S3 URLs. Published https:// paths work as-is."""

TOOL_INJECTED_CONTEXT = f"""
---
### ⚠️ CRITICAL SQL RULES (MUST FOLLOW)
1. **NO TABLES EXIST:** The database is empty. You CANNOT write `FROM table_name`.
2. **USE PARQUET PATHS:** You MUST use `FROM read_parquet('s3://...')` for ALL queries.
3. **DISCOVER PATHS — TRUST STAC PATHS EXACTLY:** Take each path from the dataset's STAC metadata (your catalog tool) and use it **verbatim**. NEVER guess, modify, or "fix" a path. Both path depth and glob pattern vary across datasets — there is no single convention. Examples:
   - `read_parquet('s3://public-wdpa/wdpa/hex/h0=*/data_0.parquet')` — partition glob
   - `read_parquet('s3://public-padus/padus-4-1/fee/hex/h0=*/data_0.parquet')` — nested path, partition glob
   - `read_parquet('s3://public-coral-reef-watch/crw-5km-annual/sst-mean/hex/h0=*/*.parquet')` — any file in each partition

   All are correct. Copy-paste the path from the catalog. In SQL examples below, `<STAC_HEX_PATH>` means "insert the exact path from the dataset's STAC metadata here."
4. **MASK BEFORE AGGREGATE (DPP rule).** When joining a small hex mask
   (e.g. state, district, county, protected-areas hexes) against a globally
   `h0`-partitioned hex dataset (land cover, climate, biomass — anything
   under `hex/h0=*/`), the `SEMI JOIN` against the mask MUST appear directly
   on the raw `read_parquet(...)` BEFORE `GROUP BY`. Aggregating the global
   side first scans every `h0` partition and runs into the query time
   limit. DuckDB dynamic partition pruning cannot push filters through
   `HASH_GROUP_BY`.
   ```sql
   SELECT a.h8, MODE(a.lc_class) AS dominant
   FROM read_parquet('<global_hex>', hive_partitioning = true) a
   SEMI JOIN <mask> m USING (h8, h0)
   WHERE a.lc_class IS NOT NULL
   GROUP BY a.h8;
   ```

### ⚡ OPTIMIZATION RULES
{OPTIM_RAW}

### 📐 H3 SPATIAL MATH
{H3_RAW}
{EXTRAS_RAW}
---
"""

# -------------------------------------------------------------------------
# 4. ISOLATION ENGINE
# -------------------------------------------------------------------------
from s3config import rewrite_sql_hrefs
# Connection setup and rendering live in queryworker, shared with the worker
# processes (#551). redact_credentials is re-exported for callers of this module.
from queryworker import TIME_LIMIT_MESSAGE, WarmWorker, error_text, isolated_db, redact_credentials, render_result  # noqa: F401
from dbconfig import (
    duckdb_max_temp_directory_size,
    duckdb_memory_limit,
    query_time_limit,
)

# Cap DuckDB's memory at ~80% of the pod's limit so an oversized query spills
# instead of OOM-killing the pod (#270); no-op unless POD_MEMORY_LIMIT /
# DUCKDB_MEMORY_LIMIT is set. Logged once at boot for ops visibility.
_MEMORY_LIMIT = duckdb_memory_limit()
if _MEMORY_LIMIT:
    print(f"DuckDB memory_limit = {_MEMORY_LIMIT} per connection (spill before cgroup OOM)",
          file=sys.stderr)
# Likewise cap each connection's spill below the ephemeral-storage limit, so an
# oversized spill fails its own query instead of getting the pod evicted (#423).
_TEMP_CAP = duckdb_max_temp_directory_size()
if _TEMP_CAP:
    print(f"DuckDB max_temp_directory_size = {_TEMP_CAP} per connection "
          f"(fail before ephemeral-storage eviction)", file=sys.stderr)


@contextmanager
def get_isolated_db(s3_key: str = None, s3_secret: str = None, s3_endpoint: str = None, s3_scope: str = None, s3_region: str = None, s3_url_style: str = None):
    """A fresh, locked-down connection for one in-process query (queryworker.py
    holds the setup, shared with the worker processes of #551)."""
    with isolated_db(SETUP_SQL, EXTRA_EXTENSIONS, s3_key=s3_key, s3_secret=s3_secret,
                     s3_endpoint=s3_endpoint, s3_scope=s3_scope, s3_region=s3_region,
                     s3_url_style=s3_url_style) as conn:
        yield conn

# -------------------------------------------------------------------------
# 5. MCP PROMPTS (Personas for Smart Clients)
# -------------------------------------------------------------------------
@mcp.prompt("geospatial-analyst")
def analyst_persona() -> str:
    return ROLE_RAW

# -------------------------------------------------------------------------
# 6. TOOL DEFINITION — SQL Query
# -------------------------------------------------------------------------
SECRET_FUNCTIONS = re.compile(r"\b(duckdb_secrets|which_secret)\b", re.IGNORECASE)




def query(sql_query: str, s3_key: str = None, s3_secret: str = None, s3_endpoint: str = None, s3_scope: str = None, s3_region: str = None, s3_url_style: str = None, stop: threading.Event = None) -> str:
    """Placeholder (overwritten below)."""
    print(f"🔍 Executing [{CALLER.get()}]: {sql_query}", file=sys.stderr)
    # Published https paths from any STAC client run as-is (#528).
    sql_query, rewrites = rewrite_sql_hrefs(sql_query)
    if rewrites:
        print(f"🔁 Rewrote {rewrites} https path(s) to s3://", file=sys.stderr)
    # duckdb_secrets() redacts the secret but shows the key ID, including one a
    # proxy injected for this query. The secrets belong to the deployment.
    if SECRET_FUNCTIONS.search(sql_query):
        return "SQL Error: duckdb_secrets() and which_secret() are not available here: the S3 secrets belong to the deployment."
    # One worker process per query (#551): it is killed at the time limit or
    # when the client goes, and its memory returns to the OS when it exits.
    if QUERY_WORKER is not None:
        return QUERY_WORKER.run(
            sql_query,
            {"s3_key": s3_key, "s3_secret": s3_secret, "s3_endpoint": s3_endpoint,
             "s3_scope": s3_scope, "s3_region": s3_region, "s3_url_style": s3_url_style},
            time_limit=query_time_limit(), stop=stop, caller=CALLER.get())
    # No worker: this module was imported (tests, tools) rather than run as the
    # server, which always starts one. Run the same code in-process, unlimited.
    try:
        with get_isolated_db(s3_key=s3_key, s3_secret=s3_secret, s3_endpoint=s3_endpoint, s3_scope=s3_scope, s3_region=s3_region, s3_url_style=s3_url_style) as db:
            return render_result(db.sql(sql_query), (s3_key, s3_secret))
    except Exception as e:
        return error_text(e, (s3_key, s3_secret))

query.__doc__ = f"""
Executes optimized DuckDB SQL against S3 parquet files.

BEFORE writing any SQL:
{_QUERY_DISCOVER_STEPS}

Private buckets are read with credentials the deployment supplies. Never pass s3_key or s3_secret, and never ask the user for keys.
For an anonymous public source (e.g. a read-only mirror), pass s3_endpoint alone (no key/secret) with s3_scope — useful to read a mirror like s3://public-* from a backup endpoint when the primary is unavailable.
Use s3_scope (a whole bucket or path, e.g. 's3://private-wyoming'; a partial name like 's3://private-' matches nothing) so DuckDB routes those paths to your endpoint rather than the server default; supply it whenever a query mixes sources.
For an anonymous AWS-hosted bucket, also pass s3_region (e.g. 'us-west-2') and, if the bucket needs it, s3_url_style ('path' by default, or 'vhost'); the defaults suit Ceph/MinIO.
WITHOUT s3_scope, your endpoint applies to EVERY s3:// path in the query and the server-default endpoint is disabled for this request — fine for a query touching only your bucket, wrong for a query mixing your bucket with catalog data. When mixing, always pass s3_scope.
{TOOL_INJECTED_CONTEXT}
"""

# query runs a DuckDB scan up to 300s. FastMCP runs a *sync* tool inline on the
# uvicorn event loop, so a long query would freeze the whole pod: /healthz goes
# unanswered (readiness pulls the pod, a long-enough scan risks the liveness
# SIGKILL), tile GETs stall, and other MCP requests queue behind it (#176). #185
# offloaded the hex tools the same way; query was the remaining sync-on-loop tool.
# A CapacityLimiter bounds concurrent scans so a burst can't oversubscribe the
# pod's CPU/memory or starve the hex tools sharing anyio's default thread pool.
#
# Each query runs in its own worker process (#551), one at a time: a heavy query
# owns the pod, and other queries go to other replicas through the idle-routing
# proxy. When several workers overlap and the pod runs out of memory, the kernel
# kills the biggest, which is the heavy query this server exists to finish. The
# spare worker is started at server start-up, not on import, so tests and tools
# that import this module start no processes.
_QUERY_LIMITER = anyio.CapacityLimiter(1)
QUERY_WORKER: WarmWorker | None = None


def _absent(value: str | None) -> str | None:
    """Treat null and the strings models write for it ("", "None", "null") as an
    omitted argument. gpt-oss sends explicit nulls for every optional argument,
    and others send the text "None"; rejecting either costs a turn (#563)."""
    if value is None or value.strip().lower() in ("", "none", "null"):
        return None
    return value


async def _query_tool(
    sql_query: str | None = None,
    s3_key: str | None = None,
    s3_secret: str | None = None,
    s3_endpoint: str | None = None,
    s3_scope: str | None = None,
    s3_region: str | None = None,
    s3_url_style: str | None = None,
    sql: str | None = None,
) -> str:
    s3_key, s3_secret, s3_endpoint, s3_scope, s3_region, s3_url_style = (
        _absent(v) for v in (s3_key, s3_secret, s3_endpoint, s3_scope, s3_region, s3_url_style))
    # Accept `sql` as an alias for `sql_query`. register_hex_tiles's required param
    # is `sql`, so in a hex workflow models reuse that name here and eat an
    # avoidable rejection+retry (#321). Whichever name arrives, run the query.
    sql_query = _absent(sql_query) or _absent(sql)
    if not sql_query:
        return "SQL Error: no query provided — pass the SQL as `sql_query` (the alias `sql` is also accepted)."
    enqueued = time.monotonic()
    stop = threading.Event()

    def _timed_query() -> str:
        started = time.monotonic()
        result = query(sql_query, s3_key, s3_secret, s3_endpoint, s3_scope,
                       s3_region, s3_url_style, stop=stop)
        outcome = "error" if result.startswith("SQL Error") else "ok"
        if result.startswith(TIME_LIMIT_MESSAGE[:40]):
            outcome = "time-limit"
        # One line per query: server-side time and time spent waiting for a
        # slot, which clients cannot see apart from network time (#519).
        print(f"⏱️ Query done [{CALLER.get()}] {outcome} in {time.monotonic() - started:.1f}s "
              f"(waited {started - enqueued:.1f}s for a slot)", file=sys.stderr)
        return result

    # With workers, a cancelled request (the client went away) stops waiting at
    # once and the stop flag kills the query's worker (#540).
    try:
        return await anyio.to_thread.run_sync(_timed_query, limiter=_QUERY_LIMITER,
                                              abandon_on_cancel=QUERY_WORKER is not None)
    except BaseException:
        stop.set()
        raise


# Register the async wrapper under the tool name, reusing the sync function's
# docstring as the LLM-facing description (mirroring the hex-tool pattern, #185).
# Name the wrapper after the PUBLIC tool so FastMCP derives a clean input-schema
# title ("queryArguments"), not "_query_toolArguments": func_metadata builds the
# args model as f"{func.__name__}Arguments", and weak models (qwen) read that
# leaked internal name off inputSchema.title and call it as if it were the tool,
# getting "Unknown tool" (#326).
_query_tool.__name__ = "query"
mcp.tool(name="query", description=query.__doc__)(_query_tool)

# s3_key and s3_secret stay in the signature, so a server-side proxy can inject
# a deployment's credentials into tools/call (tpl-internal's
# mcp-credential-proxy does), but they are left out of the schema the model
# sees. The model writes these arguments, so keys it passed would have been in
# the conversation sent to the language model. Validation uses the function's
# argument model, which still accepts them.
MODEL_HIDDEN_ARGS = ("s3_key", "s3_secret")
for _arg in MODEL_HIDDEN_ARGS:
    mcp._tool_manager.get_tool("query").parameters["properties"].pop(_arg, None)

# -------------------------------------------------------------------------
# 8b. TILE ENDPOINT — dynamic MVT for H3 hex visualization (see issue #4)
# -------------------------------------------------------------------------
import concurrent.futures
import threading
import time
from tiles.endpoint import serve_metadata, serve_tile
from tiles.db import build_tile_connection, build_tile_writer_connection, marker_connection, split_tile_writes
from tiles.pyramid import (
    MVT_LAYER_NAME,
    _public_base_url,
    prepare_hex_tiles,
    build_hex_tiles,
    cached_result_dict,
    render_recipe,
    _DISPLAY_HANDOFF_NOTE,
    _rollup_note,
    lock_is_stale,
    read_existing_metadata,
    read_failed,
    read_lock,
    tile_paths_for_hash,
    write_failed,
    write_lock,
)


# Module-level persistent connection for tile-serve GETs only: small reads of
# built tiles. It never runs caller SQL; register_hex_tiles' probes get a
# connection per call (#519). Pyramid builds get their own connections via the
# executor below so a long-running COPY can't block tile-serve reads.
_tile_con = None


def _get_tile_con():
    global _tile_con
    if _tile_con is None:
        _tile_con = build_tile_connection()
    return _tile_con


# Pod identity for cross-pod attribution in lock.json. In k8s, HOSTNAME
# is the pod name; falling back to the OS hostname for local dev.
_POD_ID = os.environ.get("HOSTNAME") or socket.gethostname()

# Background pyramid builds. Each submitted job gets a fresh DuckDB
# connection so writes don't serialise behind each other or behind reads.
_BUILD_MAX_CONCURRENCY = int(os.environ.get("TILE_BUILD_MAX_CONCURRENCY", "2"))
_BUILD_INLINE_WAIT_SECONDS = float(os.environ.get("TILE_BUILD_INLINE_WAIT_SECONDS", "5"))
# DuckDB threads for the CPU-bound pyramid build. Default 48 leaves ~16 cores
# free under the 64-core limit, so the uvicorn event loop stays schedulable and
# /healthz keeps answering during a build. (#185 removed the on-loop polling
# that was the real event-loop starvation; a pure build at 48 has ample
# headroom on a properly-sized pod — #184.) Lower this via env only on
# under-provisioned / contended nodes where the pod can't actually get ~48
# cores. Reads use TILE_THREADS (also 48) — their queries are tiny.
_BUILD_THREADS = int(os.environ.get("TILE_BUILD_THREADS", "48"))
# While a build runs, the owning pod re-writes lock.json this often so a live
# (possibly slow, thread-capped) build never looks stale; when the pod stops,
# the lock ages out within _LOCK_STALE_SECONDS. Must stay well under it.
_LOCK_HEARTBEAT_SECONDS = float(os.environ.get("TILE_LOCK_HEARTBEAT_SECONDS", "30"))
_build_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=_BUILD_MAX_CONCURRENCY,
    thread_name_prefix="tile-build",
)
_jobs_lock = threading.Lock()
# hash -> {"future": Future, "started_at": float}. Entries persist after
# completion so get_hex_tile_status can return "failed" with the error
# string; "done" status reads metadata.json directly so the job dict
# isn't authoritative for success.
_jobs: dict = {}


def _start_lock_heartbeat(output_uri: str):
    """Spawn a daemon thread that refreshes lock.json's heartbeat every
    _LOCK_HEARTBEAT_SECONDS until stopped, so a long (thread-capped) build never
    looks stale to status polls. Uses its own tiny connection — the build's
    connection is busy running the multi-minute COPY. Preserves the original
    started_at (read from the lock register wrote) so reported elapsed grows.
    Returns a stop() callable; call it when the build ends (the pod dying just
    stops the heartbeat, letting the lock age out within _LOCK_STALE_SECONDS)."""
    stop = threading.Event()
    # Writes the tile bucket, so it is a writer connection (no caller SQL).
    hb_con = build_tile_writer_connection(threads=1)
    existing = read_lock(hb_con, output_uri)
    started_at = (existing or {}).get("started_at")

    def _beat():
        try:
            while not stop.wait(_LOCK_HEARTBEAT_SECONDS):
                try:
                    write_lock(hb_con, output_uri, pod_id=_POD_ID, started_at=started_at)
                except Exception:
                    pass  # transient S3 blip; next beat retries
        finally:
            hb_con.close()

    threading.Thread(target=_beat, name="tile-lock-heartbeat", daemon=True).start()
    return stop.set


def _submit_build(plan: dict) -> concurrent.futures.Future:
    """Submit (or join) a background pyramid build for this plan. Dedups
    within-process: if a job for the same hash is already in flight, returns
    that future instead of starting a duplicate build."""
    h = plan["hash"]
    with _jobs_lock:
        existing = _jobs.get(h)
        if existing is not None and not existing["future"].done():
            return existing["future"]

        def _do_build():
            # build_con runs the caller's SQL. With a tile write key configured
            # (split_tile_writes) it holds read credentials only, and writer_con,
            # which never runs caller SQL, writes the pyramid and its markers.
            build_con = build_tile_connection(threads=_BUILD_THREADS)
            writer_con = (
                build_tile_writer_connection(threads=_BUILD_THREADS)
                if split_tile_writes() else None
            )
            print(f"[tile-build] hash={h} START pod={_POD_ID} threads={_BUILD_THREADS}"
                  f" split_writes={writer_con is not None}",
                  file=sys.stderr)
            t0 = time.perf_counter()
            stop_heartbeat = _start_lock_heartbeat(plan["output_uri"])
            try:
                if writer_con is None:
                    return build_hex_tiles(build_con, plan)
                return build_hex_tiles(build_con, plan, writer_con=writer_con)
            except Exception as exc:
                print(
                    f"[tile-build] hash={h} FAILED after "
                    f"{time.perf_counter() - t0:.1f}s: {exc}",
                    file=sys.stderr,
                )
                # Persist failure so other pods (and this pod after _jobs
                # eviction) can return status=failed instead of "unknown".
                try:
                    write_failed(writer_con or build_con, plan["output_uri"], error=str(exc))
                except Exception:
                    # Marker write failed (S3 blip); preserve original raise.
                    pass
                raise
            finally:
                stop_heartbeat()
                build_con.close()
                if writer_con is not None:
                    writer_con.close()

        future = _build_executor.submit(_do_build)
        _jobs[h] = {"future": future, "started_at": time.time()}
        return future


# Deliberate API design: only `sql` and `agg` are documented for the LLM.
# `finest_res`, `min_res`, `zoom_offset` are kept in the Python signature as
# optional kwargs for tests / REPL overrides, but NOT mentioned in the
# docstring — the MCP framework derives the LLM-facing tool schema from the
# docstring, so they stay invisible to the agent. Auto-detection (in
# tiles.pyramid.register_hex_tiles) reads the H column's resolution to set
# finest_res; min_res=2 is the coarsest level worth materializing. zoom_offset is
# now an adaptive *bias* knob: the tile endpoint picks the H3 res per zoom from
# the data's extent + cell count to hold each tile near a cell budget (#188), so
# bounded data (CA) renders finer at mid-zoom than global data does, instead of a
# single linear offset that fit neither. zoom_offset=2 is neutral (no bias);
# smaller nudges one res finer per step (legacy direction). The pyramid still
# builds all res levels, so this is a serve-time choice — no rebuild on retune.
# Adding `finest_res` etc. to the docstring is almost certainly a mistake — see
# #125 for the trigger-tightening rationale and the discussion about param surface.
def register_hex_tiles(
    sql: str,
    agg: str = "COUNT",
    finest_res: int | None = None,
    min_res: int = 2,
    zoom_offset: int = 2,
    color_scale: str = "linear",
    layer_style: str = "fill",
) -> dict:
    """Aggregate an H3 hex visualization to public object storage and return a
    paste-ready MapLibre render recipe.

    The server always builds a vector tile pyramid and hands back a ready
    `source` and `layer`. You render those verbatim.

    WHEN TO USE — only when the user explicitly asks for an aggregate
    density / heatmap / hex-grid visualization over a region. Trigger phrases:
    "hex map", "density map", "heatmap", "show density of X", "hex grid",
    "aggregate X by hex", "visualize density of X", "map the count of X per
    area".

    Call this ONLY to display a value your SQL COMPUTES that is not already a
    servable field anywhere. If the value already lives somewhere renderable,
    render that instead:
      - a raster field (effort, elevation, SST) -> the COG via titiler
      - a column already in a layer's PMTiles   -> data-driven paint (set_style)
      - a value your SQL computes, served by neither -> this tool
    It also expects per-hex values across a region, large enough to exceed the
    50-row `query` cap: a top-N answer is a `query` table, not a tile layer.
    If intent is ambiguous, ask first.

    Parameters:
    - `sql`: a SELECT whose first column is an H3 index. The tool reads that
      column's H3 resolution and uses it as the pyramid's finest level. To get
      a coarser tileset, project upstream in the SQL (e.g.
      `SELECT h3_cell_to_parent(h10, 6) AS h6, ...`).
    - `agg`: aggregation applied at each coarser pyramid level.
        - "COUNT" (default): SQL needs only the H3 column; output property
          is `count` (row count per hex). Use it for raw, un-grouped
          point/polygon rows. If your SELECT already produced one row per
          cell (you wrote `GROUP BY h<H>`), pass "SUM" to total that value up
          the pyramid, or "AVG"/"MAX" for an intensity — COUNT would count the
          single row per cell and drop your value column.
        - "AVG" / "SUM" / "MIN" / "MAX": SQL must return at least one
          numeric value column after the H3 index; each is aggregated by
          `agg` at every coarser level.
        - "COUNT_DISTINCT": SQL must return a KEY column after the H3 index
          (the thing counted distinctly, e.g. specieskey for species
          richness). Output property is that column, holding the distinct
          count per hex. Exact at the finest resolution; coarser pyramid
          levels roll up with MAX, a lower bound (see `rollup_note` in the
          result). Use this for richness maps — NOT plain COUNT, which
          measures sampling effort, not distinct species.
    - `color_scale`: "linear" (default) or "log". Controls how the viridis
      ramp is spread across the data domain in the returned recipe; the tiles
      themselves are identical either way. Use "log" for right-skewed data
      (counts, populations) where a few hot cells otherwise wash everything
      else out. `value_stats[<col>].suggested_scale` (see below) is a free
      hint telling you when "log" is worth re-requesting — re-call with the
      same SQL and `color_scale="log"` for a cache hit that just re-styles.
    - `layer_style`: "fill" (default, flat 2D) or "fill-extrusion" (3D — hex
      height also encodes the value). Like color_scale this only restyles the
      recipe, not the tiles. For 3D the map client must set pitch > 0 to see
      the extrusions; the returned `layer` is a `fill-extrusion` layer.

    Returns a dict with `status` ∈ {"done", "running", "failed"}:
    - status="done" (cache hit or fast build): the tiles exist. They are NOT
      on the map yet — display them by calling your client's map-display tool
      (`add_hex_tile_layer` in the geo-agent apps) with `tile_url` copied
      verbatim from `tile_url_template`, plus a `value_column`. `next_step` in
      the response says the same thing. Also `value_columns` and `value_stats`
      ({<col>: {"by_res": {"<res>": {"min","max","mean"}}, "suggested_scale":
      "linear"|"log"}}) if you want to customize the palette or honor the
      log-scale hint, plus `hash`, `bounds`, `feature_count_finest`.
    - status="running": being built in the background. You get `hash` and
      `tile_url_template`. Call `get_hex_tile_status(hash, wait_seconds=30)`
      to poll — it long-polls server-side, so one call returns either the
      final recipe or a single "still running" response. Do NOT retry
      register_hex_tiles with different parameters; the original build still
      finishes, and re-submitting only queues more work.
    - status="failed": build raised an error inline. `error` has the message.
      Safe to re-submit with adjusted parameters.

    If your client has no map-display tool and you are driving MapLibre
    directly, `source` and `layer` are paste-ready with a viridis ramp:
        map.addSource(id, result.source);
        map.addLayer({id: ..., source: id, ...result.layer});

    SQL patterns — pick the one matching the ask; paste exact paths from
    the dataset's STAC metadata. `<H>` is the H3 resolution, usually 8.

    1. Density (count features per hex):

       SELECT h<H> FROM read_parquet('<hex_path>') WHERE <filter>

       Call agg="COUNT". Works for pre-indexed points (GBIF) or polygons
       (PAD-US). For raw points with lat/lng:
       `h3_latlng_to_cell(lat, lng, <H>) AS h<H>`.

    2. Masked aggregate (value dataset inside a geographic mask):

       SELECT a.h<H>, AVG(a.value) AS value         -- or MODE(class) / SUM / MAX
       FROM read_parquet('<values_hex>', hive_partitioning = true) a
       SEMI JOIN read_parquet('<mask_hex>', hive_partitioning = true) b
                 USING (h<H>, h0)
       WHERE a.value IS NOT NULL
       GROUP BY a.h<H>;

       Call agg="AVG" (or the matching op). The SEMI JOIN must sit on the
       raw read_parquet(), upstream of GROUP BY — see CRITICAL rule 4 in
       the `query` description.

    3. Distinct-count per hex (e.g. species richness — distinct species
       per cell, NOT occurrence count):

       SELECT h<H>, specieskey                 -- H3 index, then the key to count
       FROM read_parquet('<hex_path>', hive_partitioning = true)
       WHERE <coordinate-quality filters>      -- see the gbif STAC data-quality note
       GROUP BY h<H>, specieskey               -- pre-dedup optional; the agg does the distinct

       Call agg="COUNT_DISTINCT". Exact at the finest resolution; coarser
       levels are a MAX-based lower bound (result carries `rollup_note`).

    Always pass hive_partitioning = true so the planner can prune h0=* files.
    """
    # Published https paths from any STAC client run as-is (#528). Before the
    # cache key is taken, so the https and s3:// forms share one pyramid.
    sql, _ = rewrite_sql_hrefs(sql)
    # A fresh connection per call, closed on return (#519). The probes run the
    # caller's SQL (the finest_res probe runs its whole aggregation). They used to
    # run on the pod-lifetime tile connection, and a long-lived DuckDB instance
    # keeps memory after a heavy query; an idle replica held 70 GiB and the
    # memory guard kept it out of rotation for hours. A closed instance gives its
    # memory back, and this one gets its own memory_limit and spill directory.
    read_con = build_tile_connection()
    try:
        plan = prepare_hex_tiles(
            con=read_con, sql=sql, agg=agg,
            finest_res=finest_res, min_res=min_res, zoom_offset=zoom_offset,
        )
        if plan["cached"] is not None:
            result = cached_result_dict(plan, plan["cached"])
            result["status"] = "done"
            return _apply_render_opts(result, color_scale, layer_style)

        # Markers are read uncached: the tile connection would keep returning
        # the first copy it read of a lock another pod keeps rewriting.
        marker_con = marker_connection().cursor()
        failed = read_failed(marker_con, plan["output_uri"])
        if failed is not None:
            return {
                "hash": plan["hash"],
                "tile_url_template": plan["tile_url_template"],
                "status": "failed",
                "error": failed.get("error", ""),
            }

        existing_lock = read_lock(marker_con, plan["output_uri"])
        if existing_lock is not None and not lock_is_stale(existing_lock):
            # Another pod owns this build. Don't submit a duplicate.
            return {
                "hash": plan["hash"],
                "tile_url_template": plan["tile_url_template"],
                "status": "running",
                "elapsed_seconds": round(time.time() - existing_lock["started_at"], 1),
            }

        try:
            if split_tile_writes():
                # read_con runs caller SQL and holds no write key; a short-lived
                # writer connection takes the lock.
                lock_con = build_tile_writer_connection(threads=1)
                try:
                    write_lock(lock_con, plan["output_uri"], pod_id=_POD_ID)
                finally:
                    lock_con.close()
            else:
                write_lock(read_con, plan["output_uri"], pod_id=_POD_ID)
        except Exception:
            # S3 blip writing lock; proceed anyway. Worst case is a duplicate
            # build elsewhere — see spec "Race we knowingly accept".
            pass

        future = _submit_build(plan)
        try:
            result = future.result(timeout=_BUILD_INLINE_WAIT_SECONDS)
            result["status"] = "done"
            return _apply_render_opts(result, color_scale, layer_style)
        except concurrent.futures.TimeoutError:
            return {
                "hash": plan["hash"],
                "tile_url_template": plan["tile_url_template"],
                "status": "running",
            }
        except Exception as e:
            return {
                "hash": plan["hash"],
                "tile_url_template": plan["tile_url_template"],
                "status": "failed",
                "error": str(e),
            }
    finally:
        read_con.close()


async def _register_hex_tiles_tool(
    sql: str = None,
    agg: str = "COUNT",
    finest_res: int | None = None,
    min_res: int = 2,
    zoom_offset: int = 2,
    color_scale: str = "linear",
    layer_style: str = "fill",
    sql_query: str = None,
) -> dict:
    # Served MCP tool. register_hex_tiles is sync (and the directly-tested core);
    # FastMCP would run a sync tool inline on the uvicorn event loop, where its
    # S3 marker I/O + bounded inline build-wait block /healthz and every other
    # request (#176). Offload it to a worker thread so the loop stays free.
    #
    # Accept `sql_query` as an alias for `sql` — the mirror of the alias `query`
    # accepts (#321) — so the two SQL tools agree on either name and models don't
    # eat a retry when they carry one name over from the other tool.
    sql = sql or sql_query
    if not sql:
        return {
            "status": "failed",
            "error": "no query provided — pass the SQL as `sql` (the alias `sql_query` is also accepted).",
        }
    return await anyio.to_thread.run_sync(
        register_hex_tiles, sql, agg, finest_res, min_res, zoom_offset, color_scale, layer_style
    )


# Register the async wrapper under the tool name, reusing the sync function's
# docstring as the LLM-facing description (the wrapper mirrors its signature).
# See the query registration above: rename the wrapper so the schema title is
# "register_hex_tilesArguments", not the leaked "_register_hex_tiles_toolArguments" (#326).
_register_hex_tiles_tool.__name__ = "register_hex_tiles"
mcp.tool(name="register_hex_tiles", description=register_hex_tiles.__doc__)(
    _register_hex_tiles_tool
)


_STATUS_POLL_MAX_WAIT_SECONDS = 60


def _apply_render_opts(result: dict, color_scale: str, layer_style: str) -> dict:
    """Re-render the recipe with non-default render options. color_scale and
    layer_style are pure render choices — they never change the tiles — so they
    are applied here at the tool boundary by rebuilding {source, layer} from the
    already-populated stats in `result`, rather than threaded through the content
    hash or build. No-op unless status="done" and at least one option is
    non-default."""
    cs = (color_scale or "linear").lower()
    ls = (layer_style or "fill").lower()
    if result.get("status") == "done" and (cs == "log" or ls == "fill-extrusion"):
        result.update(render_recipe(
            result, result["tile_url_template"], color_scale=cs, layer_style=ls,
        ))
    return result


def _done_response(base: dict, meta: dict) -> dict:
    """Build a status='done' response from either an S3 metadata dict or
    a build_hex_tiles return value — both have the same shape. Includes the
    paste-ready render recipe (source + layer) so the agent renders
    the result without further branching."""
    result = {
        **base,
        "status": "done",
        "bounds": meta["bounds"],
        "finest_res": meta["finest_res"],
        "min_res": meta["min_res"],
        "zoom_offset": meta["zoom_offset"],
        "value_columns": meta["value_columns"],
        "value_stats": meta["value_stats"],
        "layer_name": meta.get("layer_name", MVT_LAYER_NAME),
        "feature_count_finest": meta["feature_count_finest"],
        **render_recipe(meta, base["tile_url_template"]),
    }
    # Surface the rollup caveat for non-composable aggs (COUNT_DISTINCT). This
    # is the async-poll path — the common one for big builds like global GBIF
    # richness — so the note MUST appear here too, not just in the fast/cache
    # paths of register_hex_tiles (#331).
    note = _rollup_note(meta.get("agg", ""))
    if note:
        result["rollup_note"] = note
    # The async-poll path reaches "done" here rather than through pyramid.py's
    # builders, and it is the common one for big builds — the note has to be on
    # every done response or the handoff advice is missing exactly when the
    # build took long enough for the model to lose the thread (#330).
    result["next_step"] = _DISPLAY_HANDOFF_NOTE
    return result


def _status_check_once(hash: str, con=None):
    """One non-blocking status probe. Returns (kind, payload) where kind is
    "done" | "failed" | "running" | "unknown" and payload is the response dict.
    "running" means the caller should keep waiting (a local build is in flight,
    or another pod holds a fresh lock); the others are terminal for this poll.
    No sleeping here — the looping/waiting lives in the callers."""
    if con is None:
        con = marker_connection().cursor()
    paths = tile_paths_for_hash(hash)
    base = {"hash": hash, "tile_url_template": paths["tile_url_template"]}

    cached = read_existing_metadata(con, paths["output_uri"])
    if cached is not None and "bounds" in cached and "feature_count_finest" in cached:
        return ("done", _done_response(base, cached))

    failed = read_failed(con, paths["output_uri"])
    if failed is not None:
        return ("failed", {**base, "status": "failed", "error": failed.get("error", "")})

    with _jobs_lock:
        job = _jobs.get(hash)

    if job is None:
        # No local job — another pod may own this build. Consult lock.json.
        lock = read_lock(con, paths["output_uri"])
        if lock is None or lock_is_stale(lock):
            return ("unknown", {**base, "status": "unknown"})
        return ("running", {
            **base, "status": "running",
            "elapsed_seconds": round(time.time() - lock["started_at"], 1),
        })

    future = job["future"]
    if future.done():
        exc = future.exception()
        if exc is not None:
            return ("failed", {**base, "status": "failed", "error": str(exc)})
        return ("done", _done_response(base, future.result()))
    return ("running", {
        **base, "status": "running",
        "elapsed_seconds": round(time.time() - job["started_at"], 1),
    })


def get_hex_tile_status(
    hash: str, wait_seconds: int = 0, color_scale: str = "linear", layer_style: str = "fill",
) -> dict:
    """Poll the status of a pyramid build started by register_hex_tiles.

    Call this when register_hex_tiles returned {status: "running"}.

    Long-poll: pass `wait_seconds` (clamped to [0, 60]) to let the server
    block until either the build finishes OR the wait expires. Use
    wait_seconds=30 as a default — you get the final result if it finishes
    in that window, otherwise one "still running" response so you know to
    call again. Do NOT poll faster than this; rapid polling wastes turns
    without giving the build time to make progress. wait_seconds=0 returns
    immediately (the legacy non-blocking poll).

    `color_scale` ("linear" default, or "log") and `layer_style` ("fill"
    default, or "fill-extrusion") restyle the recipe returned on completion,
    exactly as in register_hex_tiles — pass them here so a build you polled
    comes back already styled, without a second register call.

    Returns one of:
    - {hash, tile_url_template, status: "done", bounds, value_stats, ...} —
      pyramid is on disk and renderable. Use bounds + value_stats with the
      map client (fit_bounds, palette domain).
    - {hash, tile_url_template, status: "running", elapsed_seconds} —
      still building. Call again with wait_seconds=30.
    - {hash, tile_url_template, status: "failed", error} — build raised.
      You may re-submit register_hex_tiles with adjusted parameters.
    - {hash, tile_url_template, status: "unknown"} — no record of this hash
      in the current server process and no completed tileset on disk. The
      hash may be for a different server, or the build may never have
      started. Re-submit register_hex_tiles to (re-)start it.

    Idempotent — safe to call repeatedly. Hash is the value returned by
    register_hex_tiles.
    """
    # Sync core, kept for direct unit testing. Loops over the one-shot check
    # with time.sleep; the served tool is the async wrapper below.
    wait_seconds = max(0, min(int(wait_seconds or 0), _STATUS_POLL_MAX_WAIT_SECONDS))
    con = marker_connection().cursor()  # markers change between polls; read uncached
    deadline = time.time() + wait_seconds
    while True:
        kind, payload = _status_check_once(hash, con)
        if kind != "running" or time.time() >= deadline:
            return _apply_render_opts(payload, color_scale, layer_style)
        time.sleep(min(2.0, max(0.1, deadline - time.time())))


async def _get_hex_tile_status_tool(
    hash: str, wait_seconds: int = 0, color_scale: str = "linear", layer_style: str = "fill",
) -> dict:
    # Served MCP tool. Truly async long-poll: `await anyio.sleep` frees the
    # event loop (vs the old time.sleep-on-the-loop that blocked /healthz and
    # every other request for up to 60s per poll — #176), and each one-shot
    # S3 check is offloaded to a worker thread.
    wait_seconds = max(0, min(int(wait_seconds or 0), _STATUS_POLL_MAX_WAIT_SECONDS))
    con = marker_connection().cursor()  # markers change between polls; read uncached
    deadline = time.time() + wait_seconds
    while True:
        kind, payload = await anyio.to_thread.run_sync(_status_check_once, hash, con)
        if kind != "running" or time.time() >= deadline:
            return _apply_render_opts(payload, color_scale, layer_style)
        await anyio.sleep(min(2.0, max(0.1, deadline - time.time())))


# Clean schema title ("get_hex_tile_statusArguments") — see query registration (#326).
_get_hex_tile_status_tool.__name__ = "get_hex_tile_status"
mcp.tool(name="get_hex_tile_status", description=get_hex_tile_status.__doc__)(
    _get_hex_tile_status_tool
)


def mount_tiles(app):
    """Mount the /tiles route onto the Starlette app and ensure tile con is ready."""
    # Pre-initialize the connection so first tile request is fast.
    con = _get_tile_con()
    app.state.tile_con = con
    app.add_route("/tiles/{namespace}/{name}/{z:int}/{x:int}/{y:int}.pbf", serve_tile)
    # Same-origin metadata sidecar so clients can read value_stats/bounds by
    # hash instead of routing the large JSON through an LLM's tool-call args.
    app.add_route("/tiles/{namespace}/{name}/metadata.json", serve_metadata, methods=["GET"])

# -------------------------------------------------------------------------
# 7. OPTIONAL BEARER TOKEN AUTH
# -------------------------------------------------------------------------
# Named tokens, one per caller (#454): MCP_AUTH_TOKENS_DIR holds one file per
# token, named for the caller (a mounted Secret). MCP_AUTH_TOKEN is the older
# single token, kept for deployments that still set it; it is named "default".
_MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "").strip()
_MCP_AUTH_TOKENS = load_tokens(os.environ.get("MCP_AUTH_TOKENS_DIR", "").strip())


def _auth_tokens() -> dict:
    tokens = dict(_MCP_AUTH_TOKENS)
    if _MCP_AUTH_TOKEN and "default" not in tokens:
        tokens["default"] = _MCP_AUTH_TOKEN
    return tokens

# Public deployments serve hex tiles without the token: the pyramids live in the
# anonymously readable public-output bucket, and browsers, GitHub Pages apps and
# exported reports fetch tile URLs directly. Off by default, because a private
# replica (e.g. wyoming, #343) serves private tiles behind the same token.
_PUBLIC_TILES = os.environ.get("MCP_AUTH_PUBLIC_TILES", "").strip().lower() in ("1", "true", "yes", "on")

class _BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # /healthz and /readyz must remain reachable to the kubelet probes even with auth on;
        # /version is deliberately public too (version discovery is the whole point).
        if request.url.path in ("/healthz", "/readyz", "/version"):
            return await call_next(request)
        if _PUBLIC_TILES and request.method == "GET" and request.url.path.startswith("/tiles/"):
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        supplied = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
        caller = match_token(supplied, _auth_tokens())
        if caller is None:
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        reset = CALLER.set(caller_label(caller, request.headers.get("X-Glen-App")))
        try:
            return await call_next(request)
        finally:
            CALLER.reset(reset)


def _start_memory_log() -> None:
    """Log the pod's memory every MEMORY_LOG_INTERVAL seconds (default 300; 0
    turns it off): the working set the OOM killer counts, the node's available
    memory, queries running, and what the shared tile connection holds. It shows
    whether memory falls back after a heavy query or stays with an idle pod (#519)."""
    try:
        interval = float(os.environ.get("MEMORY_LOG_INTERVAL", "300"))
    except ValueError:
        interval = 300.0
    if interval <= 0:
        return

    def loop():
        while True:
            time.sleep(interval)
            try:
                s = _read_memory()
                tile = _instance_memory(_tile_con) if _tile_con is not None else None
                gib = lambda b: "?" if b is None else f"{b / 2**30:.1f}"
                print(f"📊 memory: pod {gib(s.pod_used)}/{gib(s.pod_limit)} GiB, node available "
                      f"{gib(s.node_available)} GiB, queries running {_QUERY_LIMITER.borrowed_tokens}, "
                      f"tile connection {gib(tile)} GiB", file=sys.stderr)
            except Exception as e:  # logging must not die
                print(f"⚠️ memory log failed: {type(e).__name__}: {e}", file=sys.stderr)

    threading.Thread(target=loop, name="memory-log", daemon=True).start()


async def _healthz(_request):
    # Async, no executor work — fails fast if uvicorn's event loop is starved
    # (e.g. a runaway DuckDB query saturating CPU/memory on the pod). That's the
    # signal we want: a wedged pod becomes NotReady within ~15s and HAProxy
    # stops routing to it. See issue #157.
    return JSONResponse({"ok": True, "version": APP_VERSION})


async def _readyz(_request):
    # Readiness, separate from liveness (#429). Like /healthz, async and
    # I/O-free, so a starved event loop also fails it. It no longer drains on
    # memory or spill pressure: with one query per pod in its own worker (#551),
    # there is nothing to protect by turning queries away.
    return JSONResponse({"ok": True, "version": APP_VERSION})


def _public_catalog_url() -> str:
    """The catalog URL clients should use: STAC_PUBLIC_CATALOG_URL, else STAC_CATALOG_URL."""
    return (os.environ.get("STAC_PUBLIC_CATALOG_URL", "").strip()
            or os.environ.get("STAC_CATALOG_URL", "").strip())


async def _version(_request):
    # App version + git SHA of the running image (issue #221). Lets consumers and
    # ops confirm what's deployed without cluster access.
    #
    # Also the server's self-configuration surface (#346): a client that holds only
    # an `mcp_url` can GET <root>/version and learn where to fetch the catalog and
    # tiles, with no MCP session and no markdown parsing. That closes the failure
    # mode where a client's hardcoded `catalog` URL is down while this head is
    # healthy on a mirror. Only *public* URLs go here — this route is deliberately
    # auth-exempt. The server no longer reads the catalog (#528); the URL is a
    # pointer from the deployment's config, for clients such as wyoming's proxy.
    return JSONResponse({
        "version": APP_VERSION,
        "git_sha": GIT_SHA,
        "stac_catalog_url": _public_catalog_url(),
        "public_base_url": _public_base_url(),
        **({"guidance_dropped": GUIDANCE_DROPPED} if GUIDANCE_DROPPED else {}),
    })

# -------------------------------------------------------------------------
# 8. SERVER START
# -------------------------------------------------------------------------
if __name__ == "__main__":
    app = mcp.streamable_http_app()
    app.router.redirect_slashes = False
    mount_tiles(app)
    app.add_route("/healthz", _healthz, methods=["GET"])
    app.add_route("/readyz", _readyz, methods=["GET"])
    app.add_route("/version", _version, methods=["GET"])

    _tokens = _auth_tokens()
    if os.environ.get("MCP_AUTH_TOKENS_DIR", "").strip() and not _MCP_AUTH_TOKENS:
        # Fail closed: a configured but empty or unreadable token directory must
        # not start an unauthenticated server.
        print("💀 MCP_AUTH_TOKENS_DIR is set but no tokens were loaded — refusing "
              "to start without auth.", file=sys.stderr)
        sys.exit(1)
    if _tokens:
        app.add_middleware(_BearerAuthMiddleware)
        print(f"🔒 Auth enabled: {len(_tokens)} token(s) ({', '.join(sorted(_tokens))})"
              + ("; /tiles is public" if _PUBLIC_TILES else ""), file=sys.stderr)
    else:
        print("🔓 Auth disabled (no MCP_AUTH_TOKENS_DIR or MCP_AUTH_TOKEN)", file=sys.stderr)
    print(_describe_transport_security(_TRANSPORT_SECURITY), file=sys.stderr)

    QUERY_WORKER = WarmWorker(SETUP_SQL, EXTRA_EXTENSIONS)
    QUERY_WORKER.start()
    print("🧩 Query workers: one query at a time, each in a fresh process (#551)",
          file=sys.stderr)

    print("🚀 Starting DuckDB MCP Server...", file=sys.stderr)
    _start_memory_log()
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
        forwarded_allow_ips="*"
    )
