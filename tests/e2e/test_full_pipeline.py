"""The small end-to-end test: drive the whole pipeline — API job creation, dispatcher
claim, outbox publish, worker execution — exactly as the real services would, just
invoked directly (one iteration each) instead of as long-running loops, against a
real Postgres database and a real (in-memory) queue.
"""
from app.config.settings import get_settings
from app.publisher.outbox import publish_pending_batch
from app.queue.memory import InMemoryQueue
from app.scheduler.claim import claim_due_jobs
from app.workers.executor import process_message

# Deliberately in the past: the worker's own early-arrival check compares wall-clock
# "now" against this directly (independent of the dispatcher's horizon), so for the
# worker step below to actually execute rather than defer, this must already be due.
ALREADY_DUE = "2020-01-01T00:00:00Z"


async def test_job_created_via_api_runs_to_completion(client, db_session, session_factory):
    create_resp = await client.post(
        "/jobs",
        json={
            "task_type": "demo.process_data",
            "payload": {"items": [1, 2, 3]},
            "scheduled_at": ALREADY_DUE,
        },
    )
    assert create_resp.status_code == 201
    job_id = create_resp.json()["id"]

    # 1. Dispatcher: claim the due job, write the outbox row (same transaction).
    claimed = await claim_due_jobs(
        db_session, horizon_seconds=10_000_000, batch_size=10, dispatcher_id="d1"
    )
    assert len(claimed) == 1
    assert str(claimed[0].id) == job_id

    # 2. Outbox publisher: publish to the queue, job -> QUEUED.
    main_queue = InMemoryQueue()
    dlq_queue = InMemoryQueue()
    published = await publish_pending_batch(
        db_session, main_queue, batch_size=10, max_publish_attempts=5
    )
    assert published == 1

    mid_resp = await client.get(f"/jobs/{job_id}")
    assert mid_resp.json()["status"] == "QUEUED"

    # 3. Worker: receive and execute (scheduled_at is already due, so no deferral).
    [received] = await main_queue.receive(max_messages=1, wait_time_seconds=1)
    await process_message(
        received,
        session_factory=session_factory,
        main_queue=main_queue,
        dlq_queue=dlq_queue,
        settings=get_settings(),
        worker_id="worker-e2e",
    )

    final_resp = await client.get(f"/jobs/{job_id}")
    body = final_resp.json()
    assert body["status"] == "SUCCEEDED"
    assert body["completed_at"] is not None

    attempts_resp = await client.get(f"/jobs/{job_id}/attempts")
    attempts = attempts_resp.json()
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "SUCCEEDED"
