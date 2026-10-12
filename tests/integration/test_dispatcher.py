"""Integration tests for the dispatcher's claim-batch logic against real Postgres —
including a direct test that two dispatcher instances racing on the same jobs never
double-claim, which is the whole point of FOR UPDATE SKIP LOCKED."""
import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models.job import Job
from app.models.outbox import OutboxRecord, OutboxStatus
from app.models.states import JobStatus
from app.scheduler.claim import claim_due_jobs


async def _make_job(session, *, scheduled_at, status=JobStatus.SCHEDULED.value, task_type="demo.notify"):
    job = Job(task_type=task_type, payload={"recipient": "a@b.com"}, scheduled_at=scheduled_at, status=status)
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def test_claims_due_job_within_horizon(db_session):
    now = datetime.now(timezone.utc)
    job = await _make_job(db_session, scheduled_at=now + timedelta(seconds=5))
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1"
    )
    assert [j.id for j in claimed] == [job.id]
    assert claimed[0].status == JobStatus.DISPATCH_PENDING.value
    assert claimed[0].attempt_count == 1
    assert claimed[0].claimed_by == "d1"


async def test_does_not_claim_job_outside_horizon(db_session):
    now = datetime.now(timezone.utc)
    await _make_job(db_session, scheduled_at=now + timedelta(hours=5))
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1"
    )
    assert claimed == []


async def test_claims_job_scheduled_in_the_past(db_session):
    now = datetime.now(timezone.utc)
    job = await _make_job(db_session, scheduled_at=now - timedelta(hours=2))
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1"
    )
    assert [j.id for j in claimed] == [job.id]


async def test_does_not_claim_non_scheduled_jobs(db_session):
    now = datetime.now(timezone.utc)
    await _make_job(db_session, scheduled_at=now, status=JobStatus.CANCELLED.value)
    await _make_job(db_session, scheduled_at=now, status=JobStatus.RUNNING.value)
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1"
    )
    assert claimed == []


async def test_batch_size_limits_claim_count(db_session):
    now = datetime.now(timezone.utc)
    for _ in range(5):
        await _make_job(db_session, scheduled_at=now)
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=120, batch_size=2, dispatcher_id="d1"
    )
    assert len(claimed) == 2


async def test_writes_one_outbox_row_per_claimed_job(db_session):
    now = datetime.now(timezone.utc)
    job = await _make_job(db_session, scheduled_at=now)
    await claim_due_jobs(db_session, horizon_seconds=120, batch_size=10, dispatcher_id="d1")

    rows = (
        (await db_session.execute(select(OutboxRecord).where(OutboxRecord.job_id == job.id)))
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert rows[0].status == OutboxStatus.PENDING.value
    assert rows[0].attempt_number == 1
    assert rows[0].message_body["job_id"] == str(job.id)
    assert rows[0].message_body["task_type"] == "demo.notify"
    assert rows[0].message_body["attempt_number"] == 1


async def test_concurrent_dispatchers_never_double_claim(session_factory, db_session):
    """The core correctness property of SKIP LOCKED: run two independent claim
    transactions concurrently against the same pool of eligible jobs and verify every
    job is claimed exactly once, with attempt_count == 1 (not 2)."""
    now = datetime.now(timezone.utc)
    job_ids = []
    for _ in range(20):
        job = await _make_job(db_session, scheduled_at=now)
        job_ids.append(job.id)

    async def run_dispatcher(dispatcher_id: str) -> list:
        async with session_factory() as session:
            return await claim_due_jobs(
                session, horizon_seconds=120, batch_size=15, dispatcher_id=dispatcher_id
            )

    results = await asyncio.gather(run_dispatcher("d1"), run_dispatcher("d2"))
    all_claimed_ids = [j.id for batch in results for j in batch]

    assert len(all_claimed_ids) == len(job_ids)
    assert len(set(all_claimed_ids)) == len(job_ids)  # no duplicates

    async with session_factory() as session:
        rows = (
            (await session.execute(select(Job).where(Job.id.in_(job_ids))))
            .scalars()
            .all()
        )
        for row in rows:
            assert row.status == JobStatus.DISPATCH_PENDING.value
            assert row.attempt_count == 1  # claimed exactly once, not twice
