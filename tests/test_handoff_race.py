"""Deterministic tests for the statement hand-off / cancel-send protocol.

These tests exercise the real ``_Worker._execute_statements`` loop with fake
connections, so the statement-boundary race is covered without depending on
PostgreSQL signal delivery (which some sandboxes block).

The bug under test: a cancel accepted after one statement finished but
before the next one was actually sent used to be missed -- the next
statement ran anyway and the job could still be reported ``succeeded``.
The fix is an atomic claim gate (``Job.claim_next_statement``) coupled with
a cancel sender that only transmits while a statement is genuinely in
flight (``Job.send_cancel_if_in_flight``).
"""
from __future__ import annotations

import threading
import time

import pytest

from app import pool as poolmod
from app.store import Job


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.conn.executed.append(sql)


class FakeConn:
    def __init__(self):
        self.executed: list[str] = []
        self.cancels = 0

    def cursor(self):
        return FakeCursor(self)

    def cancel(self):
        self.cancels += 1


def _make_worker(conn):
    cinfo = poolmod.ConnectionInfo(0)
    cinfo.conn = conn
    worker = poolmod._Worker.__new__(poolmod._Worker)
    worker.cinfo = cinfo
    worker.pool = None
    return worker


def _make_job(n=3):
    job = Job([f"SELECT {i}" for i in range(1, n + 1)])
    assert job.mark_running(0)
    return job


class TestStatementHandoff:
    def test_cancel_in_idle_gap_never_starts_next_statement(self):
        """The reported bug: cancel accepted while the connection is idle
        between statement 1 and statement 2, before statement 2 starts."""
        job = _make_job()
        conn = FakeConn()
        worker = _make_worker(conn)
        original_gate = job.claim_next_statement

        def gate(idx):
            if idx == 1:
                # accepted while the backend is idle, immediately before the
                # claim gate is evaluated
                job.cancel_requested.set()
            return original_gate(idx)

        job.claim_next_statement = gate

        worker._execute_statements(job)
        snap = job.snapshot()

        assert conn.executed == ["SELECT 1"]
        assert conn.cancels == 0  # nothing in flight -> nothing transmitted
        assert [s["status"] for s in snap["statements"]] == [
            "ok",
            "skipped",
            "skipped",
        ]
        assert snap["status"] == "cancelled"

    def test_cancel_after_claim_is_sent_and_remaining_statements_skipped(self):
        job = _make_job()
        conn = FakeConn()
        worker = _make_worker(conn)
        original_gate = job.claim_next_statement

        def gate(idx):
            won = original_gate(idx)
            if idx == 1 and won:
                # statement 2 is now in flight
                job.cancel_requested.set()
                assert job.send_cancel_if_in_flight(conn) is True
            return won

        job.claim_next_statement = gate

        worker._execute_statements(job)
        snap = job.snapshot()

        # statement 2 completed naturally (fakes have no signals) but the
        # cancel was transmitted and statement 3 never started
        assert conn.executed == ["SELECT 1", "SELECT 2"]
        assert conn.cancels == 1
        assert [s["status"] for s in snap["statements"]] == [
            "ok",
            "ok",
            "skipped",
        ]
        assert snap["status"] == "cancelled"

    def test_sender_skips_cancel_when_connection_idle(self):
        job = _make_job(1)
        conn = FakeConn()
        # no statement in flight (worker between statements)
        assert job.statement_in_flight is False
        assert job.send_cancel_if_in_flight(conn) is False
        assert conn.cancels == 0
        # and once claimed, the same sender call does transmit
        assert job.claim_next_statement(0) is True
        assert job.send_cancel_if_in_flight(conn) is True
        assert conn.cancels == 1
        job.clear_statement_in_flight()
        # back to idle: further (late) sends are suppressed again
        assert job.send_cancel_if_in_flight(conn) is False
        assert conn.cancels == 1

    def test_terminal_job_never_receives_cancel_send(self):
        job = _make_job(1)
        conn = FakeConn()
        assert job.claim_next_statement(0) is True
        job.finish("succeeded")
        assert job.send_cancel_if_in_flight(conn) is False
        assert conn.cancels == 0

    def test_unaffected_job_runs_all_and_succeeds(self):
        job = _make_job()
        conn = FakeConn()
        worker = _make_worker(conn)
        worker._execute_statements(job)
        snap = job.snapshot()
        assert conn.executed == ["SELECT 1", "SELECT 2", "SELECT 3"]
        assert conn.cancels == 0
        assert snap["status"] == "succeeded"
        assert all(s["status"] == "ok" for s in snap["statements"])

    @pytest.mark.parametrize("trial", range(200))
    def test_gate_sender_race_fuzz(self, trial):
        """Real-thread fuzz: worker claims statements while a canceler sets
        the flag and calls the sender at random times. The gate/sender
        pairing must never both lose, and the terminal state must reflect
        what actually happened."""
        job = _make_job(2)
        conn = FakeConn()
        worker = _make_worker(conn)
        cancel_after = (trial % 20) * 0.0002  # 0 .. ~4ms spread

        def canceler():
            time.sleep(cancel_after)
            if not job.is_terminal():
                job.cancel_requested.set()
                job.send_cancel_if_in_flight(conn)

        t = threading.Thread(target=canceler)
        t.start()
        worker._execute_statements(job)
        t.join()
        snap = job.snapshot()

        assert conn.cancels in (0, 1)  # never an idle/duplicate send
        if "SELECT 2" in conn.executed:
            if job.cancel_requested.is_set():
                # cancel requested while S2 was in flight or later: no
                # further statement may run and the job is cancelled
                assert snap["status"] == "cancelled"
            else:
                assert snap["status"] == "succeeded"
        else:
            # gate caught the cancel before S2 was sent
            assert snap["status"] == "cancelled"
            assert snap["statements"][1]["status"] == "skipped"

    def test_inter_statement_gap_is_abortable(self):
        """The statement_gap_delay_ms hook pauses between statements but a
        cancel during the pause ends the job without sending the next one."""
        job = Job(
            ["SELECT 1", "SELECT 2"], statement_gap_delay_ms=5000
        )
        job.mark_running(0)
        conn = FakeConn()
        worker = _make_worker(conn)

        def canceler():
            time.sleep(0.1)
            job.cancel_requested.set()

        t = threading.Thread(target=canceler)
        t0 = time.monotonic()
        t.start()
        worker._execute_statements(job)
        t.join()
        elapsed = time.monotonic() - t0

        assert elapsed < 2.0, "gap pause was not aborted by the cancel"
        assert conn.executed == ["SELECT 1"]
        assert job.snapshot()["status"] == "cancelled"
