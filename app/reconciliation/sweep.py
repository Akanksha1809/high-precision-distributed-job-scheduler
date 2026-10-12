"""Catches the two ways a job can get stuck that nothing else in the system detects
on its own:

1. DISPATCH_PENDING with no live outbox row — the dispatcher claimed it and wrote an
   outbox row, but that row exhausted its publish-retry budget (outbox_max_publish_
   attempts) and is now permanently FAILED. The outbox publisher will never touch it
   again. Recovery: reset the job to SCHEDULED so the dispatcher claims it fresh,
   creating a brand-new outbox row and attempt.

2. RUNNING past its lease — a worker claimed the job, set lease_expires_at, and then
   crashed (container killed, OOM, network partition) before finalizing. Nothing
   else notices this; the message's own SQS visibility timeout will eventually
   expire and redeliver it, but that could be much later than our own lease, and
   isn't visible as a job-level signal. Recovery: treat it exactly like a retryable
   handler failure (reuses app.workers.outcomes.finalize_retryable_failure) — retry
   with backoff if budget remains, else dead-letter.

Both sweeps claim candidates via FOR UPDATE SKIP LOCKED so multiple reconciliation
replicas never double-recover the same job.
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.settings import Settings
from app.models.job import Job
from app.models.outbox import OutboxRecord, OutboxStatus
from app.models.states import JobStatus
from app.observability.logging import get_logger
from app.observability.metrics import reconciliation_recovered_total
from app.queue.base import Queue
from app.workers.outcomes import finalize_retryable_failure

logger = get_logger(component="reconciliation")

_ACTIVE_OUTBOX_STATUSES = (OutboxStatus.PENDING.value, OutboxStatus.PUBLISHED.value)


async def recover_stale_dispatch_pending(
    session: AsyncSession, *, stale_after_seconds: int, batch_size: int
) -> int:
    threshold = datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
    has_active_outbox_row = exists().where(
        OutboxRecord.job_id == Job.id, OutboxRecord.status.in_(_ACTIVE_OUTBOX_STATUSES)
    )
    candidate_ids = (
        await session.execute(
            select(Job.id)
            .where(
                Job.status == JobStatus.DISPATCH_PENDING.value,
                Job.dispatched_at < threshold,
                ~has_active_outbox_row,
            )
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()
    if not candidate_ids:
        return 0

    for job_id in candidate_ids:
        job = (
            await session.execute(select(Job).where(Job.id == job_id).with_for_update())
        ).scalar_one()
        if job.status != JobStatus.DISPATCH_PENDING.value:
            continue  # recovered by something else between the two queries
        job.status = JobStatus.SCHEDULED.value
        job.version += 1
        logger.warning(
            "reconciliation_recovered_stale_dispatch_pending",
            job_id=str(job.id),
            dispatched_at=job.dispatched_at.isoformat() if job.dispatched_at else None,
        )
    await session.commit()
    reconciliation_recovered_total.labels(reason="stale_dispatch_pending").inc(len(candidate_ids))
    return len(candidate_ids)


async def recover_abandoned_running(
    session: AsyncSession, *, batch_size: int, settings: Settings, dlq: Queue
) -> int:
    now = datetime.now(timezone.utc)
    candidates = (
        await session.execute(
            select(Job.id, Job.attempt_count, Job.leased_by)
            .where(Job.status == JobStatus.RUNNING.value, Job.lease_expires_at < now)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).all()
    if not candidates:
        return 0
    await session.commit()  # release the batch lock; finalize_retryable_failure re-locks per row

    recovered = 0
    for job_id, attempt_count, leased_by in candidates:
        logger.warning(
            "reconciliation_recovered_abandoned_running", job_id=str(job_id), leased_by=leased_by
        )
        await finalize_retryable_failure(
            session,
            job_id=job_id,
            attempt_number=attempt_count,
            error=f"worker lease expired (presumed crashed; last leased_by={leased_by})",
            duration_ms=0,
            timed_out=False,
            settings=settings,
            dlq=dlq,
        )
        recovered += 1
    reconciliation_recovered_total.labels(reason="abandoned_running").inc(recovered)
    return recovered


async def update_outbox_backlog_gauges(session: AsyncSession) -> None:
    from app.observability.metrics import outbox_backlog, outbox_oldest_pending_age_seconds

    pending = (
        await session.execute(
            select(OutboxRecord.created_at)
            .where(OutboxRecord.status == OutboxStatus.PENDING.value)
            .order_by(OutboxRecord.created_at.asc())
        )
    ).scalars().all()
    outbox_backlog.set(len(pending))
    if pending:
        age = (datetime.now(timezone.utc) - pending[0]).total_seconds()
        outbox_oldest_pending_age_seconds.set(max(age, 0))
    else:
        outbox_oldest_pending_age_seconds.set(0)
