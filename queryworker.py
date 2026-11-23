"""Run each `query` call in its own worker process (#551).

A worker is a separate Python process that opens one DuckDB connection, runs
one query, sends back the rendered result and exits. The server process keeps
no DuckDB state for queries, so the operating system does the cleanup that the
memory guard (#428) and the idle trim (#536) tried to do from inside:

- memory comes back when the worker exits, including what GEOS and glibc keep;
- a query past its time limit, or one whose client has gone, is stopped with
  SIGKILL, which works even inside a GEOS call that ignores DuckDB's interrupt;
- each worker raises its own oom_score_adj to 1000, so when the pod or node
  runs out of memory the kernel kills a worker, not the server.

The same module holds the connection setup and result rendering used by the
in-process path (QUERY_WORKERS unset), so both paths run identical code.

Run as a script, this file is the worker. It reads one JSON line of
configuration from stdin, prepares a connection, writes a "ready" line, then
reads one JSON request line and writes one result line. The request carries
any per-request S3 credentials, so they never appear in argv, the environment
or `ps`.
"""
import json
import os
import select
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager

import duckdb

from dbconfig import (
    connect_config,
    disable_persistent_secrets_sql,
    lock_down,
    locked_hint,
    memory_limit_sql,
    query_threads,
    spill_sql,
)
from s3config import (
    check_client_endpoint,
    default_s3_secret_sql,
    infer_use_ssl,
    source_secret_sql,
    sql_quote as _sql_quote,
)
from querytime import TIME_LIMIT_MESSAGE

# The model sees these when a worker is killed. The first matches the memory
# guard's message, so models see the same text whichever path stopped a query.
MEMORY_KILLED_MESSAGE = (
    "Query stopped: it used too much memory on this server and was interrupted so "
    "the server could keep running. Make the query hold less data at once, for "
    "example by filtering or aggregating earlier."
)
WORKER_CRASHED_MESSAGE = (
    "SQL Error: the process running this query ended unexpectedly (exit {code}). "
    "Retry once; if it happens again, simplify the query."
)
CANCELLED_MESSAGE = "Query cancelled: the client stopped waiting for it."


# ---------------------------------------------------------------------------
# Connection setup and result rendering (shared by both paths)
# ---------------------------------------------------------------------------
def redact_credentials(text: str, values) -> str:
    """Replace each credential value in text with [REDACTED]."""
    for v in values:
        if v and len(v) >= 4:
            text = text.replace(v, "[REDACTED]")
    return text


def error_text(e: Exception, secrets) -> str:
    """The text a failed query returns to the model."""
    # An S3 error can repeat the request's key ID (InvalidAccessKeyId,
    # SignatureDoesNotMatch), and this text goes back to the model.
    msg = redact_credentials(str(e), secrets)
    hint = locked_hint(msg)
    return f"SQL Error: {msg}" + (f"\n\n{hint}" if hint else "")


def check_credential_pair(s3_key, s3_secret) -> None:
    # An S3 key without its secret (or vice versa) can't authenticate. Rather
    # than silently downgrade to anonymous — which yields a confusing 403 on a
    # private bucket, or ignores the key entirely with no endpoint — fail fast
    # with the actual cause (#285). Both present = credentialed; neither present
    # = anonymous (optionally with s3_endpoint for a public mirror).
    if bool(s3_key) != bool(s3_secret):
        missing = "s3_secret" if s3_key else "s3_key"
        raise ValueError(
            f"{missing} is required: pass both s3_key and s3_secret together for "
            f"a private bucket, or neither (optionally with s3_endpoint) for an "
            f"anonymous source."
        )


def prepare_connection(setup_sql: str, extra_extensions=()) -> duckdb.DuckDBPyConnection:
    """A connection with everything that does not depend on the request: setup
    SQL, extensions, memory and spill limits, and the registry's scoped secrets.
    A worker builds this before its query arrives."""
    # Thread count and pinning are set at creation; a later SET misses running
    # workers (#509).
    conn = duckdb.connect(database=":memory:", config=connect_config(query_threads()))
    try:
        conn.sql(disable_persistent_secrets_sql())
        for stmt in (s.strip() for s in setup_sql.split(";") if s.strip()):
            try:
                conn.sql(stmt)
            except Exception as e:
                print(f"⚠️ Setup statement skipped: {stmt!r}: {e}", file=sys.stderr)
        # Work around a DuckDB `statistics_propagation` bug (#378): a SEMI/INNER
        # join whose probe side is a multi-row-group, UINT64-key-sorted parquet
        # read over S3 raises `INTERNAL Error: SetMin or SetMax ... does not match
        # statistics' column value` when the build side's key range is disjoint
        # from some row groups' zonemaps (empty intersection asserts instead of
        # pruning). Present in DuckDB 1.5.3/1.5.4; S3-transport-specific. This hits
        # real queries — masking the nhd-flowline / ACE families by a region scope
        # (the car-28 line-length cell) crashes instead of returning a number.
        # Disabling this one optimizer fixes it with no measurable perf cost (h0
        # hive-partition pruning is independent of it). An engine workaround, not
        # model guidance, so it lives here rather than in query-setup.md. Remove
        # when the upstream fix ships and the pin moves past it.
        try:
            conn.sql("SET disabled_optimizers='statistics_propagation'")
        except Exception as e:
            print(f"⚠️ disabled_optimizers setup skipped: {e}", file=sys.stderr)
        # Opt-in extras (experimental image only — empty list on the default tag).
        # Warn rather than raise: a missing extension should degrade this connection's
        # capability, not fail every query the replica serves.
        for ext in extra_extensions:
            try:
                conn.sql(f"LOAD {ext}")
            except Exception as e:
                print(f"⚠️ Extra extension {ext!r} not loaded: {e}", file=sys.stderr)
        # Bound memory to the pod so a big aggregate spills instead of OOM-killing
        # the replica (#270). Not part of SETUP_SQL: the value is deployment-derived
        # (dbconfig reads POD_MEMORY_LIMIT/DUCKDB_MEMORY_LIMIT), not model guidance.
        mem_sql = memory_limit_sql()
        if mem_sql:
            try:
                conn.sql(mem_sql)
            except Exception as e:
                print(f"⚠️ memory_limit setup skipped: {mem_sql!r}: {e}", file=sys.stderr)
        # A private, bounded spill directory for this connection (#423). After
        # SETUP_SQL so it replaces the shared '/tmp' there — concurrent spills
        # into one directory segfault the process.
        for stmt in spill_sql():
            try:
                conn.sql(stmt)
            except Exception as e:
                print(f"⚠️ spill setup skipped: {stmt!r}: {e}", file=sys.stderr)
        # Prefix-scoped secrets for every registry source (#264) — e.g. the
        # anonymous source.coop mirror. Scoped, so they coexist deterministically
        # with both the default `s3` secret and any client_s3 below (longest-
        # scope match wins); shared with the tile connection via s3config.
        for stmt in source_secret_sql():
            try:
                conn.sql(stmt)
            except Exception as e:
                print(f"⚠️ Source secret skipped: {e}", file=sys.stderr)
    except BaseException:
        conn.close()
        raise
    return conn


def finish_connection(conn, s3_key: str = None, s3_secret: str = None, s3_endpoint: str = None,
                      s3_scope: str = None, s3_region: str = None, s3_url_style: str = None) -> None:
    """Add the request's S3 secrets, then lock the connection down."""
    # Bring-your-own-bucket. Two symmetric cases, both routed through one
    # per-request `client_s3` secret:
    #   - credentialed: both s3_key and s3_secret given (private data).
    #   - anonymous:    only s3_endpoint given, no creds (a public mirror,
    #     e.g. MinIO/source.coop during a Ceph outage — #264). We key off
    #     s3_endpoint here because an anonymous bucket has no other signal.
    # Pass s3_scope (e.g. 's3://public-') so this secret applies only to those
    # paths and everything else keeps the deployment default below.
    credentialed = bool(s3_key and s3_secret)
    has_client = credentialed or bool(s3_endpoint)
    if has_client:
        endpoint = s3_endpoint or "s3-west.nrp-nautilus.io"
        check_client_endpoint(endpoint)
        key = s3_key if credentialed else ""
        secret = s3_secret if credentialed else ""
        use_ssl = infer_use_ssl(endpoint)
        # URL_STYLE defaults to 'path' (correct for Ceph/MinIO); REGION is
        # omitted unless given. A bring-your-own AWS-hosted bucket needs both
        # knobs — the built-in source_coop registry entry needed exactly
        # REGION + url_style to work (#286).
        url_style = s3_url_style or "path"
        region_clause = f", REGION '{_sql_quote(s3_region)}'" if s3_region else ""
        scope_clause = f", SCOPE '{_sql_quote(s3_scope)}'" if s3_scope else ""
        # Credentials (if any) injected here; intentionally not logged.
        conn.sql(
            f"CREATE OR REPLACE SECRET client_s3 ("
            f"TYPE S3, KEY_ID '{_sql_quote(key)}', SECRET '{_sql_quote(secret)}', "
            f"ENDPOINT '{_sql_quote(endpoint)}', URL_STYLE '{_sql_quote(url_style)}', "
            f"USE_SSL '{use_ssl}'"
            f"{region_clause}{scope_clause})"
        )
    # Default S3 endpoint — server-owned and per-deployment configurable (#268),
    # built by s3config (shared with the tile subsystem). Lets you deploy this
    # codebase as a data-access head pointed at any storage (Ceph / MinIO /
    # source.coop) purely via env, no code change.
    #
    # Skipped when client_s3 exists UNSCOPED: DuckDB's pick between two unscoped
    # secrets is an undocumented tie-break (empirically client_s3 captured every
    # path on duckdb 1.5.4, regardless of creation order — #271). Rather than
    # depend on that, make the de-facto semantic explicit and deterministic:
    # without s3_scope, the client's endpoint/creds own ALL s3:// paths for this
    # request; with s3_scope, the default serves everything outside the scope.
    if not (has_client and not s3_scope):
        try:
            conn.sql(default_s3_secret_sql())
        except Exception as e:
            print(f"⚠️ Default S3 secret setup skipped: {e}", file=sys.stderr)
    # Last: no local file access for caller SQL (NRP incident 2026-11-06).
    lock_down(conn)


@contextmanager
def isolated_db(setup_sql: str, extra_extensions=(), register=None, unregister=None,
                s3_key: str = None, s3_secret: str = None, s3_endpoint: str = None,
                s3_scope: str = None, s3_region: str = None, s3_url_style: str = None):
    """A fresh, locked-down connection for one query, closed afterwards.
    `register`/`unregister` let the in-process path report it to the memory guard."""
    check_credential_pair(s3_key, s3_secret)
    conn = prepare_connection(setup_sql, extra_extensions)
    try:
        finish_connection(conn, s3_key=s3_key, s3_secret=s3_secret, s3_endpoint=s3_endpoint,
                          s3_scope=s3_scope, s3_region=s3_region, s3_url_style=s3_url_style)
        spill_dir = register(conn) if register else None
        try:
            yield conn
        finally:
            if unregister:
                unregister(spill_dir)
    finally:
        conn.close()


def render_result(result, secrets=()) -> str:
    """Render a query result as the markdown table the model reads."""
    if result is None:
        return "Command executed successfully."

    # Drop geometry columns — GEOMETRY('OGC:CRS84') crashes pandas conversion
    # (DuckDB issue: unsupported NumPy type). Geometry is not useful in tabular output.
    geom_cols = [c for c, t in zip(result.columns, result.dtypes) if "GEOMETRY" in str(t).upper()]
    if geom_cols:
        keep = [f'"{c}"' for c in result.columns if c not in geom_cols]
        result = result.select(", ".join(keep))

    # Render temporal types as strings in DuckDB so the formatting does
    # not depend on the rest of the projection. A DuckDB DATE has no time
    # component, but pandas' to_markdown renders through df.values, which
    # flips between an ISO-with-microseconds form (all-datetime frame) and
    # a space-separated form (mixed frame) depending on the other columns
    # — neither is YYYY-MM-DD. strftime here is authoritative and
    # shape-independent (#361).
    #
    # Same treatment, same reason, for 64-bit+ integers (#387). tabulate
    # formats them through float64, which is exact only to 2^53
    # (9.007e15) — so an H3 index (~6.1e17) printed as `6.13762e+17` has
    # lost ~11 digits and names a different cell. Every hex asset carries
    # `h0`…`h15`, and h0 is BIGINT while h8/h10 are UBIGINT, so both
    # signed and unsigned must be cast. Unlike #361 this was never
    # shape-dependent: a single-column frame renders scientific too.
    # Casting is only a display change — a count still renders as `3`,
    # right-aligned, because tabulate re-parses the numeric string.
    # NULLs must become '' rather than staying NULL. Under pandas 3 a
    # str-dtype column holds missing values as the FLOAT nan, so one NULL
    # makes tabulate type the whole column as float and every wide int
    # reverts to `6.13762e+17` — the cast silently undone by a single
    # missing cell (the IUCN size-stratified assets have NULL finer
    # h-columns, so this is live). It also stops a NULL date printing as
    # the literal `nan`, which reads as a value.
    # Floats need the same treatment for the same reason, but the fix is NOT
    # the same (#410). tabulate's default `floatfmt` is `g` — six significant
    # figures — so a 26-million-acre total printed `2.64715e+07` and a dollar
    # sum `3.212e+08`. `ROUND()` does not help: this is the display layer, so
    # the obvious model-side workaround fails. Two traps here:
    #   * A VARCHAR cast alone is NOT enough. Unlike a wide int, a float string
    #     is re-parsed by tabulate and re-formatted straight back to
    #     `1.95736e+07`, so the cast must be paired with `disable_numparse` for
    #     exactly those columns. (Whole-table `disable_numparse` would undo the
    #     wide-int alignment above; a `colalign` tuple with `None` holes drops
    #     the padding entirely.)
    #   * `%.10g` rather than a plain cast, because a raw DOUBLE cast exposes
    #     representation noise — `1.74` becomes `1.7400000000000002`, and 1.74
    #     is a live gold value. DECIMAL casts exactly instead, since it is
    #     exact in DuckDB and only becomes lossy at the pandas boundary.
    # The cost is that these columns render left-aligned. Accepted: this table
    # is consumed by a model, so precision matters and alignment does not.
    # It also restores the NULL-vs-NaN distinction — both printed `nan` before,
    # while `query-optimization.md` §7 tells models to filter the NaN sentinel.
    # A NULL is now blank and a real NaN still says `nan`.
    WIDE_INTS = ("BIGINT", "UBIGINT", "HUGEINT", "UHUGEINT")  # > 2^53
    rendered = []
    no_numparse = []  # column indices tabulate must not re-parse (#410)
    for i, (c, t) in enumerate(zip(result.columns, result.dtypes)):
        tu, q = str(t).upper(), f'"{c}"'
        if tu == "DATE":
            expr = f"strftime({q}, '%Y-%m-%d')"
        elif tu.startswith("TIMESTAMP"):
            expr = f"strftime({q}, '%Y-%m-%d %H:%M:%S')"
        elif tu in WIDE_INTS:
            expr = f"CAST({q} AS VARCHAR)"
        elif tu in ("DOUBLE", "FLOAT"):
            expr = f"printf('%.10g', {q})"
            no_numparse.append(i)
        elif tu.startswith("DECIMAL"):
            expr = f"CAST({q} AS VARCHAR)"
            no_numparse.append(i)
        else:
            rendered.append(None)
            continue
        rendered.append(f"COALESCE({expr}, '') AS {q}")
    if any(rendered):
        proj = [expr or f'"{c}"' for expr, c in zip(rendered, result.columns)]
        result = result.select(", ".join(proj))

    # Fetch one extra row to detect truncation without a second COUNT scan.
    df = result.limit(51).df()
    if df.empty:
        return "No results found."
    truncated = len(df) > 50
    md = df.head(50).to_markdown(index=False, disable_numparse=no_numparse)
    if truncated:
        md += (
            "\n\n⚠️ Showing the first 50 rows only — this is a preview, not the"
            " full result and NOT a count. The true number of matching rows is"
            " larger; use COUNT(...) / COUNT(DISTINCT ...) / SUM(...) for totals."
        )
    return redact_credentials(md, secrets)


# ---------------------------------------------------------------------------
# The worker process
# ---------------------------------------------------------------------------
def _raise_oom_score() -> None:
    """Make this process the kernel OOM killer's first choice (1000 is the most
    likely), so a query that outgrows the pod or node costs itself, not the server."""
    try:
        with open("/proc/self/oom_score_adj", "w") as f:
            f.write("1000")
    except OSError:
        pass  # not Linux, or no /proc


def _worker_main() -> None:
    # Keep the real stdout for the protocol; anything else printed goes to stderr.
    out = os.fdopen(os.dup(1), "w", encoding="utf-8")
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    _raise_oom_score()

    def send(msg: dict) -> None:
        out.write(json.dumps(msg) + "\n")
        out.flush()

    line = sys.stdin.readline()
    if not line:
        os._exit(0)
    cfg = json.loads(line)
    try:
        conn = prepare_connection(cfg["setup_sql"], cfg.get("extra_extensions", ()))
        spill_dir = conn.sql("SELECT current_setting('temp_directory')").fetchone()[0]
    except Exception as e:
        send({"failed": f"{type(e).__name__}: {e}"})
        os._exit(1)
    send({"ready": True, "spill_dir": spill_dir})

    line = sys.stdin.readline()
    if not line:  # the server went away, or the pool is shutting down
        os._exit(0)
    req = json.loads(line)
    s3 = req.get("s3") or {}
    secrets = (s3.get("s3_key"), s3.get("s3_secret"))
    try:
        check_credential_pair(s3.get("s3_key"), s3.get("s3_secret"))
        finish_connection(conn, **s3)
        text = render_result(conn.sql(req["sql"]), secrets)
    except Exception as e:
        text = error_text(e, secrets)
    send({"text": text})
    # The server removes the spill directory; skip interpreter teardown.
    os._exit(0)


# ---------------------------------------------------------------------------
# The pool, in the server process
# ---------------------------------------------------------------------------
class _Worker:
    def __init__(self, proc: subprocess.Popen):
        self.proc = proc
        self.buf = b""
        self.spill_dir = None
        self.ready = False


class _Ended(Exception):
    """The worker closed its stdout: it exited or was killed."""


class WorkerPool:
    """Keeps `size` workers started and prepared, and gives each query its own.

    A worker serves one query and is replaced as soon as it is taken, so the
    start-up cost (Python, DuckDB, extensions, connection setup) is paid off the
    request path. `run()` blocks; call it from a worker thread.
    """

    TICK = 0.25  # seconds between checks of the deadline and the stop flag

    def __init__(self, size: int, setup_sql: str, extra_extensions=(),
                 start_timeout: float = 120.0, python: str = sys.executable):
        self.size = max(1, int(size))
        self._cfg = json.dumps({"setup_sql": setup_sql,
                                "extra_extensions": list(extra_extensions)}) + "\n"
        self._start_timeout = start_timeout
        self._python = python
        self._idle: deque[_Worker] = deque()
        self._running: set[_Worker] = set()  # workers with a query in flight
        self._lock = threading.Lock()
        self._closed = False

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        with self._lock:
            while len(self._idle) < self.size:
                self._idle.append(self._spawn())

    def close(self) -> None:
        with self._lock:
            self._closed = True
            idle, self._idle = list(self._idle), deque()
        for w in idle:
            self._kill(w)
            self._reap(w)

    def _spawn(self) -> _Worker:
        proc = subprocess.Popen(
            [self._python, os.path.abspath(__file__)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,
            cwd=os.path.dirname(os.path.abspath(__file__)), close_fds=True,
        )
        proc.stdin.write(self._cfg.encode())
        proc.stdin.flush()
        return _Worker(proc)

    def _take(self) -> _Worker:
        """A ready worker; starts a replacement for it."""
        for _attempt in range(3):
            with self._lock:
                if self._closed:
                    raise RuntimeError("worker pool is closed")
                w = self._idle.popleft() if self._idle else self._spawn()
                self._idle.append(self._spawn())
            try:
                msg = self._read(w, time.monotonic() + self._start_timeout)
            except (_Ended, TimeoutError) as e:
                print(f"⚠️ query worker {w.proc.pid} did not start ({type(e).__name__}); "
                      f"trying another", file=sys.stderr)
                self._kill(w)
                self._reap(w)
                continue
            if msg.get("ready"):
                w.ready, w.spill_dir = True, msg.get("spill_dir")
                return w
            print(f"⚠️ query worker {w.proc.pid} failed to prepare: {msg.get('failed')}",
                  file=sys.stderr)
            self._reap(w)
        raise RuntimeError("no query worker could start")

    # -- I/O ---------------------------------------------------------------
    def _read(self, w: _Worker, deadline: float | None, stop: threading.Event | None = None) -> dict:
        """The worker's next message. Raises TimeoutError at `deadline`,
        InterruptedError when `stop` is set, and _Ended at EOF."""
        fd = w.proc.stdout.fileno()
        while b"\n" not in w.buf:
            if stop is not None and stop.is_set():
                raise InterruptedError
            wait = self.TICK
            if deadline is not None:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError
                wait = min(wait, left)
            readable, _, _ = select.select([fd], [], [], wait)
            if readable:
                chunk = os.read(fd, 1 << 16)
                if not chunk:
                    raise _Ended
                w.buf += chunk
        line, w.buf = w.buf.split(b"\n", 1)
        return json.loads(line)

    @staticmethod
    def _kill(w: _Worker) -> None:
        try:
            w.proc.send_signal(signal.SIGKILL)
        except ProcessLookupError:
            pass

    @staticmethod
    def _reap(w: _Worker) -> int | None:
        """Wait for the worker to exit, close its pipes and remove its spill
        directory, which DuckDB leaves behind when the process is killed."""
        try:
            code = w.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            w.proc.kill()
            code = w.proc.wait()
        for f in (w.proc.stdin, w.proc.stdout):
            try:
                f.close()
            except (OSError, ValueError):
                pass
        if w.spill_dir:
            shutil.rmtree(w.spill_dir, ignore_errors=True)
        return code

    # -- one query ---------------------------------------------------------
    def running_pids(self) -> list[int]:
        """PIDs of the workers that have a query in flight."""
        with self._lock:
            return [w.proc.pid for w in self._running]

    def run(self, sql: str, s3: dict | None = None, time_limit: float | None = None,
            stop: threading.Event | None = None, caller: str = "") -> str:
        """Run one query in a worker and return the text for the model."""
        w = self._take()
        with self._lock:
            self._running.add(w)
        req = json.dumps({"sql": sql, "s3": {k: v for k, v in (s3 or {}).items() if v}}) + "\n"
        try:
            w.proc.stdin.write(req.encode())
            w.proc.stdin.flush()
            w.proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass  # it died already; the read below reports how
        deadline = time.monotonic() + time_limit if time_limit else None
        killed_for = None
        try:
            return self._read(w, deadline, stop)["text"]
        except TimeoutError:
            killed_for = "time limit"
            return TIME_LIMIT_MESSAGE.format(limit=time_limit)
        except InterruptedError:
            killed_for = "client gone"
            return CANCELLED_MESSAGE
        except _Ended:
            code = w.proc.wait()
            # SIGKILL that we did not send: the kernel OOM killer chose this worker.
            if code == -signal.SIGKILL:
                print(f"🔪 query worker {w.proc.pid} was OOM-killed [{caller}]", file=sys.stderr)
                return MEMORY_KILLED_MESSAGE
            print(f"💥 query worker {w.proc.pid} ended with exit {code} [{caller}]", file=sys.stderr)
            return WORKER_CRASHED_MESSAGE.format(code=code)
        finally:
            if killed_for:
                self._kill(w)
                print(f"🔪 query worker {w.proc.pid} killed: {killed_for} [{caller}]", file=sys.stderr)
            self._reap(w)
            with self._lock:
                self._running.discard(w)


if __name__ == "__main__":
    _worker_main()
