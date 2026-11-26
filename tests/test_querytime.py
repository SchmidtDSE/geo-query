"""Tests for the per-query time limit (#523)."""
import os
import sys
import time

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dbconfig import query_time_limit
from querytime import QueryClock, TIME_LIMIT_MESSAGE

# A cross join big enough to run for many seconds, but interruptible.
SLOW_SQL = "SELECT count(*) FROM range(100000000) a, range(100000) b WHERE a.range + b.range < 0"


def test_interrupts_a_long_query():
    con = duckdb.connect(":memory:")
    clock = QueryClock(con, SLOW_SQL, 0.5, caller="test")
    t0 = time.monotonic()
    try:
        with pytest.raises(duckdb.InterruptException):
            con.sql(SLOW_SQL).fetchall()
    finally:
        clock.stop()
    assert clock.expired.is_set()
    assert time.monotonic() - t0 < 30


def test_fast_query_is_not_interrupted():
    con = duckdb.connect(":memory:")
    clock = QueryClock(con, "SELECT 42", 5, caller="test")
    try:
        assert con.sql("SELECT 42").fetchall()[0] == (42,)
    finally:
        clock.stop()
    assert not clock.expired.is_set()


def test_logs_when_the_interrupt_does_not_take_effect(capsys):
    class Stubborn:
        def interrupt(self):
            pass  # a query that ignores the interrupt

    clock = QueryClock(Stubborn(), "SELECT\n  ST_Area(geom) FROM big", 0.1, caller="who", late_after=0.1)
    time.sleep(0.6)
    clock.stop()
    err = capsys.readouterr().err
    assert "time limit" in err and "[who]" in err
    assert "still running 0.1s after" in err
    assert "SELECT ST_Area(geom) FROM big" in err


def test_stop_cancels_the_late_check(capsys):
    class Stubborn:
        def interrupt(self):
            pass

    clock = QueryClock(Stubborn(), "SELECT 1", 0.1, late_after=0.3)
    time.sleep(0.2)
    clock.stop()  # the query returned after the interrupt
    time.sleep(0.4)
    assert "still running" not in capsys.readouterr().err


def test_message_names_the_limit():
    assert "900-second" in TIME_LIMIT_MESSAGE.format(limit=900.0)


class TestQueryTimeLimitEnv:
    def test_default_is_900(self, monkeypatch):
        monkeypatch.delenv("QUERY_TIME_LIMIT", raising=False)
        assert query_time_limit() == 900.0

    def test_zero_turns_it_off(self, monkeypatch):
        monkeypatch.setenv("QUERY_TIME_LIMIT", "0")
        assert query_time_limit() is None

    def test_value_and_bad_value(self, monkeypatch, capsys):
        monkeypatch.setenv("QUERY_TIME_LIMIT", "120")
        assert query_time_limit() == 120.0
        monkeypatch.setenv("QUERY_TIME_LIMIT", "soon")
        assert query_time_limit() == 900.0
        assert "QUERY_TIME_LIMIT ignored" in capsys.readouterr().err
