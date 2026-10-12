"""Core dispatch-claiming logic: find jobs approaching their scheduled time and move
them from SCHEDULED to DISPATCH_PENDING, writing an outbox row in the same
transaction. Safe across any number of concurrent dispatcher instances via
`FOR UPDATE SKIP LOCKED` — two dispatchers racing on the same batch window simply
end up claiming disjoint sets of rows; neither blocks the other and neither double-
claims.
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.models.outbox import OutboxRecord
from app.models.states import JobStatus
from app.observability.metrics import dispatcher_claimed_total, dispatcher_dispatch_lag_seconds


async def claim_due_jobs(
    session: AsyncSession,
    *,
    horizon_seconds: int,
    batch_size: int,
    dispatcher_id: str,
) -> list[Job]:
    """Claims up to `batch_size` SCHEDULED jobs whose scheduled_at falls within the
    next `horizon_seconds` (this also naturally picks up jobs scheduled in the past —
    the filter is `<=`, not a tight window). Commits and returns the claimed jobs.

    Uses the (status, scheduled_at) index for an index range scan — this never does a
    full table scan, and cost stays bounded by batch_size regardless of how many total
    jobs exist in the table (including far-future jobs, which the scheduled_at filter
    simply never reaches).
    """
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(seconds=horizon_seconds)

    stmt = (
        select(Job)
        .where(Job.status == JobStatus.SCHEDULED.value, Job.scheduled_at <= horizon)
        .order_by(Job.scheduled_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    claimed = list((await session.execute(stmt)).scalars().all())
    if not claimed:
        return []

    outbox_rows = []
    for job in claimed:
        lag = (now - job.scheduled_at).total_seconds()
        dispatcher_dispatch_lag_seconds.observe(max(lag, 0.0))

        job.status = JobStatus.DISPATCH_PENDING.value
        job.attempt_count += 1
        job.claimed_by = dispatcher_id
        job.claimed_at = now
        job.dispatched_at = now
        job.version += 1

        message_body = {
            "job_id": str(job.id),
            "task_type": job.task_type,
            "scheduled_at": job.scheduled_at.isoformat(),
            "attempt_number": job.attempt_count,
            "timeout_seconds": job.timeout_seconds,
        }
        outbox_rows.append(
            OutboxRecord(job_id=job.id, attempt_number=job.attempt_count, message_body=message_body)
        )

    session.add_all(outbox_rows)
    await session.commit()
    for job in claimed:
        await session.refresh(job)
    dispatcher_claimed_total.inc(len(claimed))
    return claimed
