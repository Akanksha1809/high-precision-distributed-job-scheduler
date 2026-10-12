"""Exercises app/workers/executor.process_message against real Postgres + an
in-memory queue, covering: successful execution, early-arrival deferral (no
execution, no DB mutation), permanent-failure handling, and retry-until-dead-letter
with a real DLQ message landing.

Note: every assertion re-reads job state via `db_session.refresh(job)`, not a fresh
`select()` through `db_session`. `process_message` updates the row through a
*different* session (its own `session_factory()`), and `db_session`'s identity map
would otherwise keep returning the original, now-stale, cached object for that
primary key — `refresh()` forces it to re-read the row from the database.
"""
from datetime import datetime, timedelta, timezone

from app.config.settings import get_settings
from app.models.job import Job
from app.models.states import JobStatus
from app.queue.base import OutboundMessage
from app.queue.memory import InMemoryQueue
from app.workers.executor import process_message


async def _make_queued_job(session, *, task_type, payload, attempt_count=1, max_retries=3, scheduled_at=None):
    scheduled_at = scheduled_at or datetime.now(timezone.utc)
    job = Job(
        task_type=task_type,
        payload=payload,
        scheduled_at=scheduled_at,
        status=JobStatus.QUEUED.value,
        attempt_count=attempt_count,
        max_retries=max_retries,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


def _message_for(job) -> OutboundMessage:
    return OutboundMessage(
        body={
            "job_id": str(job.id),
            "task_type": job.task_type,
            "scheduled_at": job.scheduled_at.isoformat(),
            "attempt_number": job.attempt_count,
            "timeout_seconds": job.timeout_seconds,
        },
        dedupe_key=f"{job.id}:{job.attempt_count}",
    )


async def test_successful_execution_end_to_end(db_session, session_factory):
    job = await _make_queued_job(db_session, task_type="demo.notify", payload={"recipient": "a@b.com"})
    main_q, dlq_q = InMemoryQueue(), InMemoryQueue()
    await main_q.send_batch([_message_for(job)])
    [received] = await main_q.receive(max_messages=1, wait_time_seconds=1)

    await process_message(
        received,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )

    await db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    # Message was acked (deleted) — nothing left in the queue.
    assert await main_q.receive(max_messages=10, wait_time_seconds=0.1) == []


async def test_early_message_is_deferred_not_executed(db_session, session_factory):
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    job = await _make_queued_job(
        db_session, task_type="demo.notify", payload={"recipient": "a@b.com"}, scheduled_at=future
    )
    main_q, dlq_q = InMemoryQueue(default_visibility_timeout_seconds=30), InMemoryQueue()
    await main_q.send_batch([_message_for(job)])
    [received] = await main_q.receive(max_messages=1, wait_time_seconds=1)

    await process_message(
        received,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )

    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED.value  # untouched — not executed
    # Message was deferred (change_visibility), not deleted — it's still in-flight,
    # not immediately redeliverable.
    assert await main_q.receive(max_messages=10, wait_time_seconds=0.1) == []


async def test_permanent_failure_end_to_end(db_session, session_factory):
    job = await _make_queued_job(db_session, task_type="demo.notify", payload={})  # missing recipient
    main_q, dlq_q = InMemoryQueue(), InMemoryQueue()
    await main_q.send_batch([_message_for(job)])
    [received] = await main_q.receive(max_messages=1, wait_time_seconds=1)

    await process_message(
        received,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED.value
    assert "recipient" in job.last_error


async def test_retryable_failure_then_dead_letter_end_to_end(db_session, session_factory):
    job = await _make_queued_job(
        db_session, task_type="demo.always_fail", payload={}, attempt_count=2, max_retries=2
    )
    main_q, dlq_q = InMemoryQueue(), InMemoryQueue()
    await main_q.send_batch([_message_for(job)])
    [received] = await main_q.receive(max_messages=1, wait_time_seconds=1)

    await process_message(
        received,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )

    await db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTER.value
    dlq_messages = await dlq_q.receive(max_messages=10, wait_time_seconds=1)
    assert len(dlq_messages) == 1
    assert dlq_messages[0].body["job_id"] == str(job.id)


async def test_duplicate_delivery_only_executes_once(db_session, session_factory):
    """Simulates the at-least-once-delivery scenario directly: the same message
    delivered twice (e.g. because a worker didn't delete it before its visibility
    timeout expired) must only ever actually execute the handler once."""
    job = await _make_queued_job(db_session, task_type="demo.notify", payload={"recipient": "a@b.com"})
    main_q, dlq_q = InMemoryQueue(), InMemoryQueue()
    message = _message_for(job)

    # First delivery: executes normally.
    await main_q.send_batch([message])
    [received1] = await main_q.receive(max_messages=1, wait_time_seconds=1)
    await process_message(
        received1,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )
    await db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    first_completed_at = job.completed_at

    # Second delivery of a message with the SAME attempt_number, simulating a
    # duplicate (the job itself is now SUCCEEDED, not QUEUED) — must be discarded,
    # not re-executed.
    await main_q.send_batch([message])
    [received2] = await main_q.receive(max_messages=1, wait_time_seconds=1)
    await process_message(
        received2,
        session_factory=session_factory,
        main_queue=main_q,
        dlq_queue=dlq_q,
        settings=get_settings(),
        worker_id="w1",
    )
    await db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED.value
    assert job.completed_at == first_completed_at  # unchanged — not re-run
