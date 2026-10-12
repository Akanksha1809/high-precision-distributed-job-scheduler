"""Integration tests for the reconciliation sweep — the mechanism that proves crash
recovery actually works, rather than just asserting it in prose. Each test sets up
exactly the DB state a real crash would leave behind, without ever running an actual
crashed process.
"""
from datetime import datetime, timedelta, timezone

from app.config.settings import get_settings
from app.models.attempt import AttemptOutcome, AttemptRecord
from app.models.job import Job
from app.models.outbox import OutboxRecord, OutboxStatus
from app.models.states import JobStatus
from app.queue.memory import InMemoryQueue
from app.reconciliation.sweep import (
    recover_abandoned_running,
    recover_stale_dispatch_pending,
    update_outbox_backlog_gauges,
)


async def _make_dispatch_pending_job(session, *, dispatched_at, outbox_status=None):
    job = Job(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at=dispatched_at,
        status=JobStatus.DISPATCH_PENDING.value,
        attempt_count=1,
        dispatched_at=dispatched_at,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    if outbox_status is not None:
        session.add(
            OutboxRecord(
                job_id=job.id,
                attempt_number=1,
                message_body={"job_id": str(job.id)},
                status=outbox_status,
            )
        )
        await session.commit()
    return job


async def test_recovers_dispatch_pending_with_no_live_outbox_row(db_session):
    old = datetime.now(timezone.utc) - timedelta(seconds=120)
    job = await _make_dispatch_pending_job(db_session, dispatched_at=old, outbox_status=OutboxStatus.FAILED.value)

    n = await recover_stale_dispatch_pending(db_session, stale_after_seconds=60, batch_size=100)
    assert n == 1

    await db_session.refresh(job)
    assert job.status == JobStatus.SCHEDULED.value


async def test_does_not_touch_dispatch_pending_with_live_outbox_row(db_session):
    """The publisher is just behind, not stuck — reconciliation must leave this alone,
    or it would cause a duplicate re-dispatch of a job that's about to be published."""
    old = datetime.now(timezone.utc) - timedelta(seconds=120)
    job = await _make_dispatch_pending_job(db_session, dispatched_at=old, outbox_status=OutboxStatus.PENDING.value)

    n = await recover_stale_dispatch_pending(db_session, stale_after_seconds=60, batch_size=100)
    assert n == 0

    await db_session.refresh(job)
    assert job.status == JobStatus.DISPATCH_PENDING.value


async def test_does_not_touch_recently_dispatched_job(db_session):
    recent = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = await _make_dispatch_pending_job(db_session, dispatched_at=recent, outbox_status=OutboxStatus.FAILED.value)

    n = await recover_stale_dispatch_pending(db_session, stale_after_seconds=60, batch_size=100)
    assert n == 0

    await db_session.refresh(job)
    assert job.status == JobStatus.DISPATCH_PENDING.value


async def _make_abandoned_running_job(session, *, attempt_count=1, max_retries=3, lease_age_seconds=200):
    now = datetime.now(timezone.utc)
    job = Job(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at=now,
        status=JobStatus.RUNNING.value,
        attempt_count=attempt_count,
        max_retries=max_retries,
        leased_by="worker-that-crashed",
        lease_expires_at=now - timedelta(seconds=lease_age_seconds),
        started_at=now - timedelta(seconds=lease_age_seconds + 10),
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    session.add(
        AttemptRecord(
            job_id=job.id,
            attempt_number=attempt_count,
            worker_id="worker-that-crashed",
            outcome=AttemptOutcome.RUNNING.value,
        )
    )
    await session.commit()
    return job


async def test_recovers_abandoned_running_job_with_retry_budget_remaining(db_session):
    job = await _make_abandoned_running_job(db_session, attempt_count=1, max_retries=3)
    dlq = InMemoryQueue()

    n = await recover_abandoned_running(
        db_session, batch_size=100, settings=get_settings(), dlq=dlq
    )
    assert n == 1

    await db_session.refresh(job)
    assert job.status == JobStatus.SCHEDULED.value
    assert job.scheduled_at > datetime.now(timezone.utc)
    assert "lease expired" in job.last_error


async def test_recovers_abandoned_running_job_to_dead_letter_when_budget_exhausted(db_session):
    job = await _make_abandoned_running_job(db_session, attempt_count=3, max_retries=3)
    dlq = InMemoryQueue()

    n = await recover_abandoned_running(
        db_session, batch_size=100, settings=get_settings(), dlq=dlq
    )
    assert n == 1

    await db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTER.value
    dlq_messages = await dlq.receive(max_messages=10, wait_time_seconds=1)
    assert len(dlq_messages) == 1


async def test_does_not_touch_running_job_with_active_lease(db_session):
    job = await _make_abandoned_running_job(db_session, attempt_count=1, lease_age_seconds=-60)
    dlq = InMemoryQueue()

    n = await recover_abandoned_running(
        db_session, batch_size=100, settings=get_settings(), dlq=dlq
    )
    assert n == 0

    await db_session.refresh(job)
    assert job.status == JobStatus.RUNNING.value


async def test_outbox_backlog_gauges_do_not_error_and_reflect_pending_count(db_session):
    job = await _make_dispatch_pending_job(
        db_session,
        dispatched_at=datetime.now(timezone.utc),
        outbox_status=OutboxStatus.PENDING.value,
    )
    await update_outbox_backlog_gauges(db_session)

    from app.observability.metrics import outbox_backlog

    assert outbox_backlog._value.get() >= 1  # noqa: SLF001 - reading the gauge's current value directly
    assert job.id is not None  # keep the job referenced; silences unused-var linters
