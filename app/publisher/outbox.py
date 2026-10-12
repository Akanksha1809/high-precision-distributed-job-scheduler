"""The other half of the transactional outbox pattern: reads PENDING outbox rows
(written atomically by the dispatcher alongside each job's SCHEDULED -> DISPATCH_PENDING
transition) and actually publishes them to the queue.

This is deliberately a separate process/step from the dispatcher. If this step dies
after calling queue.send_batch() but before committing PUBLISHED, the row is still
PENDING and will be retried — resulting in a duplicate message, not a lost one. If it
dies before calling send_batch() at all, nothing changed and the row is retried
cleanly. Either way: at-least-once, never silently dropped. Workers are responsible
for tolerating the resulting duplicates (see app/workers).
"""
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.models.outbox import OutboxRecord, OutboxStatus
from app.models.states import JobStatus
from app.observability.logging import get_logger
from app.observability.metrics import (
    outbox_publish_exhausted_total,
    outbox_publish_failed_total,
    outbox_published_total,
)
from app.queue.base import OutboundMessage, Queue

logger = get_logger(component="outbox_publisher")


async def publish_pending_batch(
    session: AsyncSession,
    queue: Queue,
    *,
    batch_size: int,
    max_publish_attempts: int,
) -> int:
    """Claims up to `batch_size` PENDING outbox rows, publishes them in one queue
    batch call, and commits the outcome (including the DISPATCH_PENDING -> QUEUED
    job transition for whichever rows actually succeeded) in a single transaction.
    Returns the number of rows processed (success + failure)."""
    stmt = (
        select(OutboxRecord)
        .where(OutboxRecord.status == OutboxStatus.PENDING.value)
        .order_by(OutboxRecord.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    if not rows:
        return 0

    messages = [
        OutboundMessage(body=row.message_body, dedupe_key=f"{row.job_id}:{row.attempt_number}")
        for row in rows
    ]
    results_by_key = {r.dedupe_key: r for r in await queue.send_batch(messages)}

    now = datetime.now(timezone.utc)
    succeeded_job_ids = []
    for row in rows:
        key = f"{row.job_id}:{row.attempt_number}"
        result = results_by_key.get(key)
        row.publish_attempts += 1
        if result is not None and result.success:
            row.status = OutboxStatus.PUBLISHED.value
            row.published_at = now
            row.last_error = None
            succeeded_job_ids.append(row.job_id)
            outbox_published_total.inc()
        else:
            row.last_error = result.error if result else "queue returned no result for this message"
            if row.publish_attempts >= max_publish_attempts:
                row.status = OutboxStatus.FAILED.value
                outbox_publish_exhausted_total.inc()
                logger.error(
                    "outbox_publish_exhausted",
                    outbox_id=str(row.id),
                    job_id=str(row.job_id),
                    attempts=row.publish_attempts,
                )
            else:
                row.status = OutboxStatus.PENDING.value  # retried on a later poll
            outbox_publish_failed_total.inc()

    if succeeded_job_ids:
        job_stmt = select(Job).where(Job.id.in_(succeeded_job_ids)).with_for_update()
        jobs = (await session.execute(job_stmt)).scalars().all()
        for job in jobs:
            if job.status == JobStatus.DISPATCH_PENDING.value:
                job.status = JobStatus.QUEUED.value
                job.queued_at = now
                job.version += 1

    await session.commit()
    return len(rows)
