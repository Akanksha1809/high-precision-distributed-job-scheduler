"""Integration tests for the outbox publisher, including the resilience property
that is the entire point of the transactional outbox: a job claimed by the
dispatcher but not yet published survives an arbitrarily long gap (simulating a
publisher crash/restart) with zero data loss, because the outbox row is itself
durable, committed state in Postgres — not something held in a process's memory."""
from datetime import datetime, timezone

from sqlalchemy import select

from app.models.job import Job
from app.models.outbox import OutboxRecord, OutboxStatus
from app.models.states import JobStatus
from app.publisher.outbox import publish_pending_batch
from app.queue.base import OutboundMessage, Queue, SendResult
from app.queue.memory import InMemoryQueue
from app.scheduler.claim import claim_due_jobs


class AlwaysFailQueue(Queue):
    async def send_batch(self, messages: list[OutboundMessage]) -> list[SendResult]:
        return [SendResult(dedupe_key=m.dedupe_key, success=False, error="simulated outage") for m in messages]

    async def receive(self, max_messages: int, wait_time_seconds: int):
        return []

    async def delete(self, receipt_handle: str) -> None:
        pass

    async def change_visibility(self, receipt_handle: str, visibility_timeout_seconds: int) -> None:
        pass


async def _make_scheduled_job(session):
    job = Job(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at=datetime.now(timezone.utc),
        status=JobStatus.SCHEDULED.value,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def test_publish_moves_outbox_published_and_job_queued(db_session):
    job = await _make_scheduled_job(db_session)
    await claim_due_jobs(db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1")

    queue = InMemoryQueue()
    processed = await publish_pending_batch(
        db_session, queue, batch_size=10, max_publish_attempts=5
    )
    assert processed == 1

    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED.value
    assert job.queued_at is not None

    outbox_row = (
        (await db_session.execute(select(OutboxRecord).where(OutboxRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert outbox_row.status == OutboxStatus.PUBLISHED.value
    assert outbox_row.published_at is not None

    # And the message really did land on the queue, ready for a worker.
    received = await queue.receive(max_messages=10, wait_time_seconds=1)
    assert len(received) == 1
    assert received[0].body["job_id"] == str(job.id)


async def test_outbox_survives_publisher_outage_with_no_data_loss(db_session):
    """Claim a job (dispatcher), then simulate the publisher being down/crashed for
    an arbitrary period by simply not calling it. The job must stay safely at
    DISPATCH_PENDING with its outbox row intact — nothing is lost, nothing silently
    half-applied. When the publisher finally runs, it completes normally."""
    job = await _make_scheduled_job(db_session)
    await claim_due_jobs(db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1")

    await db_session.refresh(job)
    assert job.status == JobStatus.DISPATCH_PENDING.value
    outbox_row = (
        (await db_session.execute(select(OutboxRecord).where(OutboxRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert outbox_row.status == OutboxStatus.PENDING.value

    # "The publisher process was down" is simulated by elapsed time with no calls.
    # Nothing to assert here beyond: the row above is still there, untouched.

    queue = InMemoryQueue()
    processed = await publish_pending_batch(
        db_session, queue, batch_size=10, max_publish_attempts=5
    )
    assert processed == 1
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED.value


async def test_publish_failure_retries_then_leaves_job_dispatch_pending(db_session):
    job = await _make_scheduled_job(db_session)
    await claim_due_jobs(db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1")

    queue = AlwaysFailQueue()
    processed = await publish_pending_batch(
        db_session, queue, batch_size=10, max_publish_attempts=5
    )
    assert processed == 1

    await db_session.refresh(job)
    assert job.status == JobStatus.DISPATCH_PENDING.value  # never advanced to QUEUED

    outbox_row = (
        (await db_session.execute(select(OutboxRecord).where(OutboxRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert outbox_row.status == OutboxStatus.PENDING.value  # eligible for retry
    assert outbox_row.publish_attempts == 1
    assert outbox_row.last_error == "simulated outage"


async def test_publish_failure_exhausts_to_failed_after_max_attempts(db_session):
    job = await _make_scheduled_job(db_session)
    await claim_due_jobs(db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1")

    queue = AlwaysFailQueue()
    for _ in range(3):
        await publish_pending_batch(db_session, queue, batch_size=10, max_publish_attempts=3)

    outbox_row = (
        (await db_session.execute(select(OutboxRecord).where(OutboxRecord.job_id == job.id)))
        .scalars()
        .one()
    )
    assert outbox_row.status == OutboxStatus.FAILED.value
    assert outbox_row.publish_attempts == 3

    await db_session.refresh(job)
    assert job.status == JobStatus.DISPATCH_PENDING.value
