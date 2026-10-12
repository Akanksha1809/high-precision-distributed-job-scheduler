"""Job lifecycle state machine.

Design note: a RETRY_PENDING job becomes SCHEDULED again (with scheduled_at bumped to
the next backoff time) rather than having its own re-dispatch path. This lets retries
reuse the exact same dispatcher/outbox/queue/worker pipeline as a fresh job instead of
a parallel code path that would need its own testing and could drift out of sync.
"""
from __future__ import annotations

import enum


class JobStatus(str, enum.Enum):
    SCHEDULED = "SCHEDULED"
    DISPATCH_PENDING = "DISPATCH_PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_PENDING = "RETRY_PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


TERMINAL_STATES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED}
)

# Legal state transitions. Anything not listed here is rejected.
_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SCHEDULED: frozenset({JobStatus.DISPATCH_PENDING, JobStatus.CANCELLED}),
    JobStatus.DISPATCH_PENDING: frozenset(
        {JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.CANCELLED}
    ),
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.RETRY_PENDING, JobStatus.FAILED, JobStatus.DEAD_LETTER}
    ),
    JobStatus.RETRY_PENDING: frozenset({JobStatus.SCHEDULED, JobStatus.CANCELLED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.DEAD_LETTER: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

# States from which a client-initiated cancel is accepted. RUNNING is intentionally
# excluded: we have no safe way to interrupt a handler already executing, so cancelling
# a RUNNING job would just be a lie. Document this as best-effort cancellation.
CANCELLABLE_STATES: frozenset[JobStatus] = frozenset(
    {JobStatus.SCHEDULED, JobStatus.DISPATCH_PENDING, JobStatus.QUEUED, JobStatus.RETRY_PENDING}
)

# States from which a client-initiated reschedule is accepted. Excluded: DISPATCH_PENDING,
# QUEUED, RUNNING (a message may already be in flight for the old scheduled_at — racing a
# reschedule against that is how you get a worker executing a job at the wrong time).
RESCHEDULABLE_STATES: frozenset[JobStatus] = frozenset(
    {
        JobStatus.SCHEDULED,
        JobStatus.RETRY_PENDING,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.DEAD_LETTER,
    }
)


def can_transition(frm: JobStatus, to: JobStatus) -> bool:
    return to in _TRANSITIONS.get(frm, frozenset())


class IllegalTransitionError(Exception):
    def __init__(self, frm: JobStatus, to: JobStatus):
        self.frm = frm
        self.to = to
        super().__init__(f"Illegal job state transition: {frm.value} -> {to.value}")
