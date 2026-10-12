"""Per-message processing: parse -> defer-if-early -> atomically claim -> execute the
registered handler with a timeout -> finalize (success/permanent-fail/retry-or-DLQ)
-> ack the queue message. The queue message is only ever deleted after the outcome is
durably committed to Postgres, so a worker crash mid-execution just leaves the
message to redeliver once its visibility timeout expires (reconciliation also
catches this independently by lease_expires_at — see app/reconciliation).
"""
import asyncio
import time
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config.settings import Settings
from app.handlers.base import PermanentError, RetryableError
from app.handlers.registry import get_handler
from app.observability.logging import get_logger
from app.observability.metrics import (
    messages_deferred_total,
    stale_messages_discarded_total,
    task_execution_duration_seconds,
    worker_active_tasks,
)
from app.queue.base import Queue, ReceivedMessage
from app.workers.claim import claim_for_execution
from app.workers.outcomes import (
    finalize_permanent_failure,
    finalize_retryable_failure,
    finalize_success,
)

logger = get_logger(component="worker")

# Cap how long we ever ask the queue to hide a message for, regardless of how far out
# its scheduled_at is. Bounds worst-case redelivery latency if our own clock or the
# job's scheduled_at is further off than expected, and stays comfortably under SQS's
# 12-hour change_message_visibility ceiling.
_MAX_DEFER_SECONDS = 900


async def process_message(
    msg: ReceivedMessage,
    *,
    session_factory: async_sessionmaker,
    main_queue: Queue,
    dlq_queue: Queue,
    settings: Settings,
    worker_id: str,
) -> None:
    try:
        job_id = UUID(msg.body["job_id"])
        attempt_number = int(msg.body["attempt_number"])
        scheduled_at = datetime.fromisoformat(msg.body["scheduled_at"])
    except (KeyError, ValueError) as exc:
        logger.error("malformed_message_discarded", error=str(exc), body=msg.body)
        await main_queue.delete(msg.receipt_handle)
        stale_messages_discarded_total.labels(reason="malformed").inc()
        return

    now = datetime.now(timezone.utc)
    if now < scheduled_at:
        # Arrived early (the dispatcher works within a horizon well before the exact
        # scheduled time — see app/scheduler/claim.py). Defer instead of executing or
        # busy-waiting: hide the message until close to its due time, then let the
        # normal receive loop pick it up again and re-check.
        defer_seconds = min((scheduled_at - now).total_seconds(), _MAX_DEFER_SECONDS)
        await main_queue.change_visibility(
            msg.receipt_handle, visibility_timeout_seconds=max(int(defer_seconds), 1)
        )
        messages_deferred_total.inc()
        return

    async with session_factory() as session:
        claim = await claim_for_execution(
            session,
            job_id=job_id,
            message_attempt_number=attempt_number,
            worker_id=worker_id,
            lease_duration_seconds=settings.job_lease_duration_seconds,
        )
    if claim.job is None:
        await main_queue.delete(msg.receipt_handle)
        stale_messages_discarded_total.labels(reason=claim.discard_reason).inc()
        return

    job = claim.job
    worker_active_tasks.inc()
    start = time.perf_counter()
    outcome_kind: str
    error_message: str | None = None
    try:
        handler = get_handler(job.task_type)
    except KeyError as exc:
        outcome_kind = "permanent"
        error_message = str(exc)
    else:
        try:
            await asyncio.wait_for(handler(job.payload), timeout=job.timeout_seconds)
            outcome_kind = "success"
        except TimeoutError:
            outcome_kind = "timeout"
            error_message = f"handler exceeded timeout_seconds={job.timeout_seconds}"
        except PermanentError as exc:
            outcome_kind = "permanent"
            error_message = str(exc)
        except RetryableError as exc:
            outcome_kind = "retryable"
            error_message = str(exc)
        except Exception as exc:  # noqa: BLE001 - fail-safe: treat unknown errors as retryable
            outcome_kind = "retryable"
            error_message = f"unexpected handler exception ({type(exc).__name__}): {exc}"
            logger.warning(
                "handler_raised_non_contractual_exception", task_type=job.task_type, error=error_message
            )
    duration = time.perf_counter() - start
    task_execution_duration_seconds.labels(task_type=job.task_type).observe(duration)
    worker_active_tasks.dec()
    duration_ms = int(duration * 1000)

    async with session_factory() as session:
        if outcome_kind == "success":
            await finalize_success(
                session, job_id=job_id, attempt_number=attempt_number, duration_ms=duration_ms
            )
        elif outcome_kind == "permanent":
            await finalize_permanent_failure(
                session,
                job_id=job_id,
                attempt_number=attempt_number,
                error=error_message or "permanent failure",
                duration_ms=duration_ms,
            )
        else:
            await finalize_retryable_failure(
                session,
                job_id=job_id,
                attempt_number=attempt_number,
                error=error_message or "retryable failure",
                duration_ms=duration_ms,
                timed_out=(outcome_kind == "timeout"),
                settings=settings,
                dlq=dlq_queue,
            )

    await main_queue.delete(msg.receipt_handle)
