"""Integration tests against a real Postgres database (tests/conftest.py points at
the `chronos_test` database in the docker-compose `postgres` service)."""
import uuid

FUTURE = "2026-12-01T00:00:00Z"
FAR_FUTURE = "2027-06-01T00:00:00Z"


async def test_health_and_ready(client):
    r = await client.get("/health")
    assert r.status_code == 200
    r = await client.get("/ready")
    assert r.status_code == 200


async def test_create_and_get_job(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "SCHEDULED"
    assert body["task_type"] == "demo.notify"

    r2 = await client.get(f"/jobs/{body['id']}")
    assert r2.status_code == 200
    assert r2.json()["id"] == body["id"]


async def test_get_nonexistent_job_404(client):
    r = await client.get(f"/jobs/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_create_job_rejects_unregistered_task_type(client):
    r = await client.post(
        "/jobs", json={"task_type": "nope", "payload": {}, "scheduled_at": FUTURE}
    )
    assert r.status_code == 422


async def test_create_job_rejects_naive_datetime(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {}, "scheduled_at": "2026-12-01T00:00:00"},
    )
    assert r.status_code == 422


async def test_create_job_rejects_oversized_payload(client):
    r = await client.post(
        "/jobs",
        json={
            "task_type": "demo.notify",
            "payload": {"blob": "x" * (300 * 1024)},
            "scheduled_at": FUTURE,
        },
    )
    assert r.status_code == 422


async def test_idempotency_key_returns_same_job(client):
    payload = {
        "task_type": "demo.notify",
        "payload": {"recipient": "a@b.com"},
        "scheduled_at": FUTURE,
        "idempotency_key": "idem-abc",
    }
    r1 = await client.post("/jobs", json=payload)
    r2 = await client.post("/jobs", json=payload)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["id"] == r2.json()["id"]


async def test_idempotency_key_conflict_on_different_body(client):
    key = "idem-conflict"
    r1 = await client.post(
        "/jobs",
        json={
            "task_type": "demo.notify",
            "payload": {"recipient": "a@b.com"},
            "scheduled_at": FUTURE,
            "idempotency_key": key,
        },
    )
    assert r1.status_code == 201
    r2 = await client.post(
        "/jobs",
        json={
            "task_type": "demo.notify",
            "payload": {"recipient": "DIFFERENT@b.com"},
            "scheduled_at": FUTURE,
            "idempotency_key": key,
        },
    )
    assert r2.status_code == 409


async def test_list_jobs_pagination(client):
    for _ in range(5):
        await client.post(
            "/jobs",
            json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
        )
    r = await client.get("/jobs?limit=2")
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 2
    assert body["next_cursor"] is not None

    r2 = await client.get(f"/jobs?limit=2&cursor={body['next_cursor']}")
    assert r2.status_code == 200
    body2 = r2.json()
    assert len(body2["items"]) == 2
    # No overlap between pages.
    ids_page1 = {j["id"] for j in body["items"]}
    ids_page2 = {j["id"] for j in body2["items"]}
    assert ids_page1.isdisjoint(ids_page2)


async def test_list_jobs_filter_by_status(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    job_id = r.json()["id"]
    await client.post(f"/jobs/{job_id}/cancel")

    r2 = await client.get("/jobs?status=CANCELLED")
    assert r2.status_code == 200
    assert any(j["id"] == job_id for j in r2.json()["items"])

    r3 = await client.get("/jobs?status=SCHEDULED")
    assert all(j["id"] != job_id for j in r3.json()["items"])


async def test_cancel_job(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    job_id = r.json()["id"]
    r2 = await client.post(f"/jobs/{job_id}/cancel")
    assert r2.status_code == 200
    assert r2.json()["status"] == "CANCELLED"


async def test_cancel_already_cancelled_job_409(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    job_id = r.json()["id"]
    await client.post(f"/jobs/{job_id}/cancel")
    r2 = await client.post(f"/jobs/{job_id}/cancel")
    assert r2.status_code == 409


async def test_cancel_nonexistent_job_404(client):
    r = await client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert r.status_code == 404


async def test_reschedule_job(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    job_id = r.json()["id"]
    r2 = await client.post(f"/jobs/{job_id}/reschedule", json={"scheduled_at": FAR_FUTURE})
    assert r2.status_code == 200
    body = r2.json()
    assert body["status"] == "SCHEDULED"
    assert body["scheduled_at"] == FAR_FUTURE


async def test_reschedule_nonexistent_job_404(client):
    r = await client.post(f"/jobs/{uuid.uuid4()}/reschedule", json={"scheduled_at": FAR_FUTURE})
    assert r.status_code == 404


async def test_attempts_empty_for_new_job(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.notify", "payload": {"recipient": "a@b.com"}, "scheduled_at": FUTURE},
    )
    job_id = r.json()["id"]
    r2 = await client.get(f"/jobs/{job_id}/attempts")
    assert r2.status_code == 200
    assert r2.json() == []


async def test_attempts_for_nonexistent_job_404(client):
    r = await client.get(f"/jobs/{uuid.uuid4()}/attempts")
    assert r.status_code == 404


async def test_metrics_endpoint_exposes_prometheus_format(client):
    r = await client.get("/metrics")
    assert r.status_code == 200
    assert "chronos_jobs_created_total" in r.text
