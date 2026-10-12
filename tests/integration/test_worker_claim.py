import uuid
from datetime import datetime, timezone

from app.models.job import Job
from app.models.states import JobStatus
from app.workers.claim import claim_for_execution


async def _make_job(session, *, status, attempt_count=1):
    job = Job(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at=datetime.now(timezone.utc),
        status=status,
        attempt_count=attempt_count,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def test_claims_queued_job(db_session):
    job = await _make_job(db_session, status=JobStatus.QUEUED.value, attempt_count=1)
    result = await claim_for_execution(
        db_session, job_id=job.id, message_attempt_number=1, worker_id="w1", lease_duration_seconds=90
    )
    assert result.job is not None
    assert result.job.status == JobStatus.RUNNING.value
    assert result.job.leased_by == "w1"
    assert result.job.lease_expires_at is not None


async def test_rejects_cancelled_job(db_session):
    job = await _make_job(db_session, status=JobStatus.CANCELLED.value, attempt_count=1)
    result = await claim_for_execution(
        db_session, job_id=job.id, message_attempt_number=1, worker_id="w1", lease_duration_seconds=90
    )
    assert result.job is None
    assert result.discard_reason == "cancelled"


async def test_rejects_stale_attempt_number(db_session):
    job = await _make_job(db_session, status=JobStatus.QUEUED.value, attempt_count=2)
    # message carries attempt_number=1, but the job has since been redispatched to attempt 2
    result = await claim_for_execution(
        db_session, job_id=job.id, message_attempt_number=1, worker_id="w1", lease_duration_seconds=90
    )
    assert result.job is None
    assert result.discard_reason == "stale_attempt"


async def test_rejects_duplicate_delivery_of_already_running_job(db_session):
    job = await _make_job(db_session, status=JobStatus.RUNNING.value, attempt_count=1)
    result = await claim_for_execution(
        db_session, job_id=job.id, message_attempt_number=1, worker_id="w2", lease_duration_seconds=90
    )
    assert result.job is None
    assert result.discard_reason == "unexpected_status_RUNNING"


async def test_rejects_duplicate_delivery_of_already_succeeded_job(db_session):
    job = await _make_job(db_session, status=JobStatus.SUCCEEDED.value, attempt_count=1)
    result = await claim_for_execution(
        db_session, job_id=job.id, message_attempt_number=1, worker_id="w2", lease_duration_seconds=90
    )
    assert result.job is None
    assert result.discard_reason == "unexpected_status_SUCCEEDED"


async def test_rejects_nonexistent_job(db_session):
    result = await claim_for_execution(
        db_session,
        job_id=uuid.uuid4(),
        message_attempt_number=1,
        worker_id="w1",
        lease_duration_seconds=90,
    )
    assert result.job is None
    assert result.discard_reason == "job_not_found"
