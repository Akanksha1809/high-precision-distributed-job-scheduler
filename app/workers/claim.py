"""Atomic QUEUED -> RUNNING claim, with the staleness checks that make at-least-once
delivery safe: a worker must never execute a job twice for the same attempt, never
execute a job that was cancelled after being queued, and never execute a message left
over from a dispatch attempt that's since been superseded (job rescheduled and
redispatched with a new attempt_count).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.attempt import AttemptOutcome, AttemptRecord
from app.models.job import Job
from app.models.states import JobStatus


@dataclass(frozen=True, slots=True)
class ClaimResult:
    job: Job | None
    discard_reason: str | None  # set (and job=None) when the message should be dropped


async def claim_for_execution(
    session: AsyncSession,
    *,
    job_id,
    message_attempt_number: int,
    worker_id: str,
    lease_duration_seconds: int,
) -> ClaimResult:
    job = (
        await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    ).scalar_one_or_none()

    if job is None:
        await session.commit()
        return ClaimResult(job=None, discard_reason="job_not_found")

    if job.status == JobStatus.CANCELLED.value:
        await session.commit()
        return ClaimResult(job=None, discard_reason="cancelled")

    if job.attempt_count != message_attempt_number:
        # The job has moved on (rescheduled/redispatched since this message was
        # published) — this message is a leftover from a superseded attempt.
        await session.commit()
        return ClaimResult(job=None, discard_reason="stale_attempt")

    if job.status != JobStatus.QUEUED.value:
        # Either a duplicate concurrent delivery of the same message (another worker
        # already claimed it and it's now RUNNING) or a duplicate delivered after the
        # job already reached a terminal state. Either way: don't re-execute.
        await session.commit()
        return ClaimResult(job=None, discard_reason=f"unexpected_status_{job.status}")

    now = datetime.now(timezone.utc)
    job.status = JobStatus.RUNNING.value
    job.started_at = now
    job.leased_by = worker_id
    job.lease_expires_at = now + timedelta(seconds=lease_duration_seconds)
    job.version += 1

    session.add(
        AttemptRecord(
            job_id=job.id,
            attempt_number=message_attempt_number,
            worker_id=worker_id,
            outcome=AttemptOutcome.RUNNING.value,
            started_at=now,
        )
    )
    await session.commit()
    await session.refresh(job)
    return ClaimResult(job=job, discard_reason=None)
