import base64
import hashlib
import json
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.attempt import AttemptRecord
from app.models.job import Job
from app.models.states import (
    CANCELLABLE_STATES,
    RESCHEDULABLE_STATES,
    JobStatus,
)


class IdempotencyConflictError(Exception):
    """Same idempotency_key reused with a different request body."""


class JobNotFoundError(Exception):
    pass


class IllegalStateForOperationError(Exception):
    def __init__(self, current_status: str, operation: str):
        self.current_status = current_status
        self.operation = operation
        super().__init__(f"Cannot {operation} job in status {current_status}")


def _request_hash(task_type: str, payload: dict, scheduled_at: datetime) -> str:
    canonical = json.dumps(
        {
            "task_type": task_type,
            "payload": payload,
            "scheduled_at": scheduled_at.astimezone(timezone.utc).isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def create_job(
    session: AsyncSession,
    *,
    task_type: str,
    payload: dict,
    scheduled_at: datetime,
    idempotency_key: str | None,
    max_retries: int,
    timeout_seconds: int,
) -> tuple[Job, bool]:
    """Returns (job, created). created=False means an existing job with the same
    idempotency_key was returned instead of creating a new one."""
    req_hash = _request_hash(task_type, payload, scheduled_at)

    if idempotency_key:
        existing = (
            await session.execute(select(Job).where(Job.idempotency_key == idempotency_key))
        ).scalar_one_or_none()
        if existing is not None:
            if existing.idempotency_request_hash != req_hash:
                raise IdempotencyConflictError(
                    f"idempotency_key {idempotency_key!r} was already used with a "
                    "different request body"
                )
            return existing, False

    job = Job(
        task_type=task_type,
        payload=payload,
        scheduled_at=scheduled_at,
        idempotency_key=idempotency_key,
        idempotency_request_hash=req_hash if idempotency_key else None,
        status=JobStatus.SCHEDULED.value,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        # Race: another request with the same key committed between our SELECT and
        # INSERT. Re-fetch and treat it the same as the up-front hit above.
        if idempotency_key:
            existing = (
                await session.execute(select(Job).where(Job.idempotency_key == idempotency_key))
            ).scalar_one_or_none()
            if existing is not None:
                if existing.idempotency_request_hash != req_hash:
                    raise IdempotencyConflictError(
                        f"idempotency_key {idempotency_key!r} was already used with a "
                        "different request body"
                    ) from None
                return existing, False
        raise
    await session.refresh(job)
    return job, True


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if job is None:
        raise JobNotFoundError(f"job {job_id} not found")
    return job


def _encode_cursor(created_at: datetime, job_id: uuid.UUID) -> str:
    raw = f"{created_at.astimezone(timezone.utc).isoformat()}|{job_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    created_at_str, job_id_str = raw.split("|", 1)
    return datetime.fromisoformat(created_at_str), uuid.UUID(job_id_str)


async def list_jobs(
    session: AsyncSession,
    *,
    status: str | None = None,
    task_type: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    """Cursor-paginated, newest first. Cursor is opaque (created_at, id) so pages stay
    stable even as new jobs are inserted concurrently."""
    stmt = select(Job)
    if status:
        stmt = stmt.where(Job.status == status)
    if task_type:
        stmt = stmt.where(Job.task_type == task_type)
    if cursor:
        created_at, job_id = _decode_cursor(cursor)
        stmt = stmt.where(
            (Job.created_at < created_at)
            | ((Job.created_at == created_at) & (Job.id < job_id))
        )
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars().all())
    next_cursor = None
    if len(rows) > limit:
        last_kept = rows[limit - 1]
        next_cursor = _encode_cursor(last_kept.created_at, last_kept.id)
        rows = rows[:limit]
    return rows, next_cursor


async def list_attempts(session: AsyncSession, job_id: uuid.UUID) -> list[AttemptRecord]:
    stmt = (
        select(AttemptRecord)
        .where(AttemptRecord.job_id == job_id)
        .order_by(AttemptRecord.attempt_number.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def cancel_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = (
        await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    ).scalar_one_or_none()
    if job is None:
        raise JobNotFoundError(f"job {job_id} not found")
    if JobStatus(job.status) not in CANCELLABLE_STATES:
        raise IllegalStateForOperationError(job.status, "cancel")
    job.status = JobStatus.CANCELLED.value
    job.version += 1
    await session.commit()
    await session.refresh(job)
    return job


async def reschedule_job(session: AsyncSession, job_id: uuid.UUID, scheduled_at: datetime) -> Job:
    job = (
        await session.execute(select(Job).where(Job.id == job_id).with_for_update())
    ).scalar_one_or_none()
    if job is None:
        raise JobNotFoundError(f"job {job_id} not found")
    if JobStatus(job.status) not in RESCHEDULABLE_STATES:
        raise IllegalStateForOperationError(job.status, "reschedule")
    job.status = JobStatus.SCHEDULED.value
    job.scheduled_at = scheduled_at
    job.dispatched_at = None
    job.queued_at = None
    job.started_at = None
    job.completed_at = None
    job.last_error = None
    job.dead_letter_reason = None
    job.version += 1
    await session.commit()
    await session.refresh(job)
    return job
