from datetime import datetime, timezone

from sqlalchemy import select

from app.config.settings import get_settings
from app.models.attempt import AttemptOutcome, AttemptRecord
from app.models.job import Job
from app.models.states import JobStatus
from app.queue.memory import InMemoryQueue
from app.workers.outcomes import (
    finalize_permanent_failure,
    finalize_retryable_failure,
    finalize_success,
)


async def _make_running_job(session, *, attempt_count=1, max_retries=3):
    job = Job(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at=datetime.now(timezone.utc),
        status=JobStatus.RUNNING.value,
        attempt_count=attempt_count,
        max_retries=max_retries,
        leased_by="w1",
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    session.add(
        AttemptRecord(
            job_id=job.id,
            attempt_number=attempt_count,
            worker_id="w1",
            outcome=AttemptOutcome.RUNNING.value,
        )
    )
    await session.commit()
    return job


async def test_finalize_success(db_session):
    job = await _make_running_job(db_session)
    await finalize_success(db_session, job_id=job.id, attempt_number=1, duration_ms=42)
    await db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.completed_at is not None
    assert job.leased_by is None

    attempt = (
        (await db_session.execute(select(AttemptRecord).where(AttemptRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert attempt.outcome == AttemptOutcome.SUCCEEDED.value
    assert attempt.duration_ms == 42


async def test_finalize_permanent_failure(db_session):
    job = await _make_running_job(db_session)
    await finalize_permanent_failure(
        db_session, job_id=job.id, attempt_number=1, error="bad payload", duration_ms=5
    )
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED.value
    assert job.last_error == "bad payload"
    assert job.leased_by is None


async def test_retryable_failure_schedules_retry_when_budget_remains(db_session):
    job = await _make_running_job(db_session, attempt_count=1, max_retries=3)
    settings = get_settings()
    dlq = InMemoryQueue()
    await finalize_retryable_failure(
        db_session,
        job_id=job.id,
        attempt_number=1,
        error="downstream timeout",
        duration_ms=10,
        timed_out=False,
        settings=settings,
        dlq=dlq,
    )
    await db_session.refresh(job)
    assert job.status == JobStatus.SCHEDULED.value
    assert job.scheduled_at > datetime.now(timezone.utc)
    assert job.last_error == "downstream timeout"
    # Nothing should have gone to the DLQ — retries remain.
    assert await dlq.receive(max_messages=10, wait_time_seconds=0.1) == []


async def test_retryable_failure_dead_letters_when_budget_exhausted(db_session):
    job = await _make_running_job(db_session, attempt_count=3, max_retries=3)
    settings = get_settings()
    dlq = InMemoryQueue()
    await finalize_retryable_failure(
        db_session,
        job_id=job.id,
        attempt_number=3,
        error="downstream still down",
        duration_ms=10,
        timed_out=False,
        settings=settings,
        dlq=dlq,
    )
    await db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTER.value
    assert job.dead_letter_reason == "downstream still down"

    dlq_messages = await dlq.receive(max_messages=10, wait_time_seconds=1)
    assert len(dlq_messages) == 1
    assert dlq_messages[0].body["job_id"] == str(job.id)


async def test_timeout_outcome_recorded_distinctly(db_session):
    job = await _make_running_job(db_session, attempt_count=1, max_retries=3)
    settings = get_settings()
    dlq = InMemoryQueue()
    await finalize_retryable_failure(
        db_session,
        job_id=job.id,
        attempt_number=1,
        error="timed out",
        duration_ms=30000,
        timed_out=True,
        settings=settings,
        dlq=dlq,
    )
    attempt = (
        (await db_session.execute(select(AttemptRecord).where(AttemptRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert attempt.outcome == AttemptOutcome.TIMED_OUT.value
