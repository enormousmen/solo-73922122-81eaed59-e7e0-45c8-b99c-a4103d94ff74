"""In-memory job registry.

Jobs live in memory only (diagnostic service; nothing to persist across
restarts).  All mutable fields are guarded by a per-job lock; the two
``threading.Event`` flags are written by the API/cancel threads and read by
the worker threads.
"""
from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Job:
    def __init__(self, statements: list[str], cleanup_delay_ms: int | None = None,
                 break_connection_after: bool = False,
                 statement_gap_delay_ms: int | None = None):
        self.id = uuid.uuid4().hex[:12]
        self.lock = threading.Lock()
        # Serializes the statement hand-off: the worker takes this lock to
        # atomically (re)check the cancel flag *and* mark a statement as
        # in flight; the cancel sender takes it to decide whether the
        # connection actually has an in-flight statement to interrupt.
        # Lock order when both are needed: handoff_lock -> lock.
        self.handoff_lock = threading.Lock()
        self.status = "queued"  # queued -> running -> succeeded|failed|cancelled
        self.created_at = utcnow()
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.connection_id: int | None = None
        self.cleanup_delay_ms = cleanup_delay_ms  # test hook; None = server default
        self.break_connection_after = break_connection_after  # test hook
        self.statement_gap_delay_ms = statement_gap_delay_ms  # test hook
        self.error: str | None = None
        # cancel plumbing
        self.cancel_requested = threading.Event()
        self.cancel_send_done = threading.Event()
        self.cancel_accepted_at: str | None = None
        # True only between the atomic claim gate and the end of execute();
        # guarded by handoff_lock. When False the connection is idle (between
        # statements, in cleanup or not yet started) and conn.cancel() would
        # have no statement to interrupt.
        self.statement_in_flight = False
        # set when the job reaches a terminal state
        self.done = threading.Event()
        self.records = [
            {
                "index": i,
                "sql": sql,
                "status": "pending",  # pending|running|ok|error|cancelled|skipped
                "started_at": None,
                "duration_ms": None,
                "columns": None,
                "rows": None,
                "rowcount": None,
                "truncated": False,
                "error": None,
            }
            for i, sql in enumerate(statements)
        ]

    # ---- mutations (worker / API threads) ---------------------------------

    def mark_running(self, connection_id: int) -> bool:
        with self.lock:
            if self.status in TERMINAL_STATES:
                return False
            self.status = "running"
            self.started_at = utcnow()
            self.connection_id = connection_id
            return True

    # ---- statement hand-off / cancel-send protocol ------------------------

    def claim_next_statement(self, idx: int) -> bool:
        """Atomically gate the start of statement ``idx``.

        Returns ``True`` and marks the statement in flight only if no cancel
        was accepted before the statement actually starts. If a cancel was
        accepted while the connection was still idle (e.g. in the gap after
        the previous statement finished), returns ``False`` and the statement
        must never be sent to PostgreSQL.

        Together with :meth:`send_cancel_if_in_flight` this closes the
        statement-boundary race: a cancel flag set and an in-flight marker
        can never be observed as mutually exclusive in a way that both (a)
        lets the statement start and (b) skips the cancel signal.
        """
        with self.handoff_lock:
            if self.cancel_requested.is_set():
                return False
            self.statement_in_flight = True
            return True

    def clear_statement_in_flight(self) -> None:
        with self.handoff_lock:
            self.statement_in_flight = False

    def send_cancel_if_in_flight(self, conn) -> bool:
        """Transmit ``conn.cancel()`` iff a statement is currently in flight.

        Returns ``True`` when the cancel was sent, ``False`` when the
        connection was idle between statements (or the job had ended): in
        that case nothing is transmitted -- a cancel delivered while the
        backend is idle would be discarded by PostgreSQL before the next
        statement runs -- and the worker's claim gate stops the next
        statement instead. A late cancel can therefore never disturb the
        next job on the same connection.
        """
        with self.handoff_lock:
            with self.lock:
                terminal = self.status in TERMINAL_STATES
            if terminal or not self.statement_in_flight or conn is None:
                return False
            conn.cancel()
            return True

    def stmt_running(self, idx: int) -> None:
        with self.lock:
            rec = self.records[idx]
            rec["status"] = "running"
            rec["started_at"] = utcnow()

    def stmt_ok(self, idx, columns, rows, rowcount, truncated, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="ok",
                columns=columns,
                rows=rows,
                rowcount=rowcount,
                truncated=truncated,
                duration_ms=duration_ms,
            )

    def stmt_error(self, idx, error, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="error", error=error, duration_ms=duration_ms
            )

    def stmt_cancelled(self, idx, error, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="cancelled", error=error, duration_ms=duration_ms
            )

    def skip_from(self, idx: int) -> None:
        with self.lock:
            for rec in self.records[idx:]:
                if rec["status"] == "pending":
                    rec["status"] = "skipped"

    def finish(self, status: str, error: str | None = None) -> bool:
        """Transition to a terminal state.  First terminal transition wins."""
        with self.lock:
            if self.status in TERMINAL_STATES:
                return False
            self.status = status
            self.finished_at = utcnow()
            self.error = error
            self.done.set()
            return True

    # ---- reads -------------------------------------------------------------

    def is_terminal(self) -> bool:
        with self.lock:
            return self.status in TERMINAL_STATES

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "connection_id": self.connection_id,
                "cleanup_delay_ms": self.cleanup_delay_ms,
                "statement_gap_delay_ms": self.statement_gap_delay_ms,
                "error": self.error,
                "cancel_requested": self.cancel_requested.is_set(),
                "cancel_accepted_at": self.cancel_accepted_at,
                "statements": [dict(rec) for rec in self.records],
            }

    def summary(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "connection_id": self.connection_id,
                "num_statements": len(self.records),
            }


class JobStore:
    """Thread-safe registry keeping the most recent ``capacity`` jobs."""

    def __init__(self, capacity: int):
        self._lock = threading.Lock()
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._capacity = capacity

    def add(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.id] = job
            while len(self._jobs) > self._capacity:
                self._jobs.popitem(last=False)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_summaries(self, limit: int = 200) -> list[dict]:
        with self._lock:
            jobs = list(self._jobs.values())
        return [job.summary() for job in reversed(jobs[-limit:])]
