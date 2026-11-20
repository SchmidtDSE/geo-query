"""Per-query time limit for the `query` tool (#523)."""
import sys
import threading


class QueryClock:
    """Interrupt a query that runs past QUERY_TIME_LIMIT seconds (#523).

    Clients give up after about 10 minutes, so a query running far past that
    serves no one; one ran 9 hours on a pod out of rotation. The interrupt
    relies on DuckDB checking for it, so a second timer logs when a query is
    still running a minute after it was interrupted.
    """

    def __init__(self, conn, sql: str, limit: float, caller: str = "", late_after: float = 60.0):
        self.conn = conn
        self.sql = sql
        self.limit = limit
        self.late_after = late_after
        self.expired = threading.Event()
        self.done = threading.Event()
        self._caller = caller
        self._timers = [threading.Timer(limit, self._expire)]
        for t in self._timers:
            t.daemon = True
            t.start()

    def _first_line(self) -> str:
        return " ".join(self.sql.split())[:120]

    def _expire(self) -> None:
        if self.done.is_set():
            return
        self.expired.set()
        print(f"⏰ query time limit {self.limit:g}s reached [{self._caller}]; interrupting: "
              f"{self._first_line()}", file=sys.stderr)
        try:
            self.conn.interrupt()
        except Exception as e:
            print(f"⚠️ interrupt failed: {type(e).__name__}: {e}", file=sys.stderr)
        late = threading.Timer(self.late_after, self._still_running)
        late.daemon = True
        self._timers.append(late)
        late.start()

    def _still_running(self) -> None:
        if not self.done.is_set():
            print(f"🚨 query still running {self.late_after:g}s after its time-limit interrupt [{self._caller}]: "
                  f"{self._first_line()}", file=sys.stderr)

    def stop(self) -> None:
        self.done.set()
        for t in self._timers:
            t.cancel()


TIME_LIMIT_MESSAGE = (
    "SQL Error: the query ran longer than this server's {limit:g}-second limit and was stopped. "
    "Narrow it: filter early, include h0 in joins, aggregate the hex tables rather than raw "
    "geometries, or scope the area."
)
