"""Memory readings for the pod's periodic memory log (#519).

The pod's working set and limit come from cgroup v2, the node's available
memory from /proc/meminfo, and a DuckDB instance's own figure from
duckdb_memory(). Nothing acts on these: query workers (#551) bound a query's
memory by process, and the kernel reclaims it when the worker exits.
"""
import os
from typing import NamedTuple

from dbconfig import _parse_bytes


class MemorySample(NamedTuple):
    pod_used: int | None        # working set: what the OOM killer and kubelet count
    pod_limit: int | None
    node_available: int | None  # MemAvailable for the whole node


def read_memory(cgroup: str = "/sys/fs/cgroup", meminfo: str = "/proc/meminfo") -> MemorySample:
    """The pod's working set and limit (cgroup v2) and the node's available memory."""
    used = limit = available = None
    try:
        with open(os.path.join(cgroup, "memory.current")) as f:
            current = int(f.read())
        inactive_file = 0
        with open(os.path.join(cgroup, "memory.stat")) as f:
            for line in f:
                if line.startswith("inactive_file "):
                    inactive_file = int(line.split()[1])
                    break
        used = max(current - inactive_file, 0)
    except (OSError, ValueError):
        pass
    try:
        with open(os.path.join(cgroup, "memory.max")) as f:
            raw = f.read().strip()
        limit = None if raw == "max" else int(raw)
    except (OSError, ValueError):
        pass
    if limit is None:
        raw = os.environ.get("POD_MEMORY_LIMIT", "").strip()
        try:
            limit = (_parse_bytes(raw) or None) if raw else None
        except ValueError:
            limit = None
    try:
        with open(meminfo) as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    available = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError):
        pass
    return MemorySample(used, limit, available)


def instance_memory(conn) -> int | None:
    """Bytes DuckDB holds for *conn*'s database instance, read while it runs.

    A cursor is a second connection to the same instance, so it can read
    duckdb_memory() during a query. It is closed at once: an open cursor would
    keep the instance, its memory and its spill files alive after the query's own
    connection closes.

    EXTERNAL_FILE_CACHE is left out: on a remote Parquet read it reserves the
    whole file (4.3 GiB for wdpa.parquet) as the query starts, while the process
    holds a fraction of that, and it misled the old memory guard (#530).
    """
    try:
        cur = conn.cursor()
    except Exception:
        return None  # closed
    try:
        return int(cur.sql("SELECT coalesce(sum(memory_usage_bytes), 0) "
                           "FROM duckdb_memory() WHERE tag <> 'EXTERNAL_FILE_CACHE'").fetchall()[0][0])
    except Exception:
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
