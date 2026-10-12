"""What happens to a job after its handler returns, raises, or times out. Each
function is a self-contained, independently testable DB operation — this is
deliberately decoupled from queue/message handling so retry-backoff and
dead-letter-routing logic can be tested directly against Postgres without needing a
running queue or executing a real handler.
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.settings import Settings
from app.models.attempt import AttemptOutcome, AttemptRecord
from app.models.job import Job
from app.models.states import JobStatus
from app.observability.logging import get_logger
from app.observability.metrics import (
    jobs_dead_lettered_total,
    jobs_failed_permanent_total,
    jobs_retried_total,
    jobs_succeeded_total,
    jobs_timed_out_total,
)
from app.queue.base import OutboundMessage, Queue
from app.workers.retry import compute_backoff_seconds

logger = get_logger(component="worker")


async def _load_job_and_attempt(
    session: AsyncSession, job_id, attempt_number: int
) -> tuple[Job, AttemptRecord]:
    job = (
        await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    ).scalar_one()
    attempt = (
        await session.execute(
            select(AttemptRecord).where(
                AttemptRecord.job_id == job_id, AttemptRecord.attempt_number == attempt_number
            )
        )
    ).scalar_one()
    return job, attempt


async def finalize_success(session: AsyncSession, *, job_id, attempt_number: int, duration_ms: int) -> None:
    job, attempt = await _load_job_and_attempt(session, job_id, attempt_number)
    now = datetime.now(timezone.utc)

    attempt.outcome = AttemptOutcome.SUCCEEDED.value
    attempt.completed_at = now
    attempt.duration_ms = duration_ms

    job.status = JobStatus.SUCCEEDED.value
    job.completed_at = now
    job.last_error = None
    job.leased_by = None
    job.lease_expires_at = None
    job.version += 1

    await session.commit()
    jobs_succeeded_total.labels(task_type=job.task_type).inc()


async def finalize_permanent_failure(
    session: AsyncSession, *, job_id, attempt_number: int, error: str, duration_ms: int
) -> None:
    job, attempt = await _load_job_and_attempt(session, job_id, attempt_number)
    now = datetime.now(timezone.utc)

    attempt.outcome = AttemptOutcome.FAILED_PERMANENT.value
    attempt.completed_at = now
    attempt.duration_ms = duration_ms
    attempt.error_message = error

    job.status = JobStatus.FAILED.value
    job.completed_at = now
    job.last_error = error
    job.leased_by = None
    job.lease_expires_at = None
    job.version += 1

    await session.commit()
    jobs_failed_permanent_total.labels(task_type=job.task_type).inc()


async def finalize_retryable_failure(
    session: AsyncSession,
    *,
    job_id,
    attempt_number: int,
    error: str,
    duration_ms: int,
    timed_out: bool,
    settings: Settings,
    dlq: Queue,
) -> None:
    """Either schedules the next retry (with backoff+jitter) or, if the retry budget
    is exhausted, routes the job to DEAD_LETTER and publishes it to the DLQ for
    operator inspection. Documented policy: a timeout is treated as a retryable
    failure like any other, up to the same retry budget — see FAILURE_MODES.md.
    """
    job, attempt = await _load_job_and_attempt(session, job_id, attempt_number)
    now = datetime.now(timezone.utc)

    attempt.outcome = AttemptOutcome.TIMED_OUT.value if timed_out else AttemptOutcome.FAILED_RETRYABLE.value
    attempt.completed_at = now
    attempt.duration_ms = duration_ms
    attempt.error_message = error

    job.last_error = error
    job.leased_by = None
    job.lease_expires_at = None

    if timed_out:
        jobs_timed_out_total.labels(task_type=job.task_type).inc()

    if job.attempt_count >= job.max_retries:
        job.status = JobStatus.DEAD_LETTER.value
        job.dead_letter_reason = error
        job.completed_at = now
        job.version += 1
        await session.commit()
        jobs_dead_lettered_total.labels(task_type=job.task_type).inc()

        dlq_body = {
            "job_id": str(job.id),
            "task_type": job.task_type,
            "attempt_count": job.attempt_count,
            "error": error,
            "failed_at": now.isoformat(),
        }
        try:
            await dlq.send_batch(
                [OutboundMessage(body=dlq_body, dedupe_key=f"{job.id}:{job.attempt_count}:dlq")]
            )
        except Exception:
            # The job's DB state (DEAD_LETTER) is already durable and correct even if
            # this best-effort DLQ notification fails — operators can still find it
            # via GET /jobs?status=DEAD_LETTER. Log and move on rather than retry
            # loop here, which would risk re-running the already-finalized job.
            logger.error("dlq_publish_failed", job_id=str(job.id), exc_info=True)
        return

    # RUNNING -> RETRY_PENDING is committed on its own first so the state is actually
    # observable (if only briefly) rather than being an implementation detail that
    # never appears outside this function — see app/models/states.py.
    job.status = JobStatus.RETRY_PENDING.value
    job.version += 1
    await session.commit()

    delay_seconds = compute_backoff_seconds(
        job.attempt_count,
        base_delay_seconds=settings.retry_base_delay_seconds,
        max_delay_seconds=settings.retry_max_delay_seconds,
        jitter_seconds=settings.retry_jitter_seconds,
    )
    job.status = JobStatus.SCHEDULED.value
    job.scheduled_at = now + timedelta(seconds=delay_seconds)
    job.version += 1
    await session.commit()
    jobs_retried_total.labels(task_type=job.task_type).inc()
