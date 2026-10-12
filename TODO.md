# Chronos — TODO / Progress Log

## Milestones

- [x] M1 — Vertical slice: repo scaffold, config, DB models, Alembic migration, FastAPI
      `POST /jobs` + `GET /jobs/{id}` + `GET /health`, job state machine, unit tests.
- [ ] M2 — Full job CRUD: list/filter/paginate, cancel, reschedule, attempts history,
      idempotency keys, integration tests against real Postgres (docker compose).
- [x] M3 — Dispatcher + transactional outbox + queue abstraction + in-memory queue impl
      (ended up building the real Queue interface now rather than stubbing it — see
      notes below). SQS/LocalStack backend deferred to M4 as planned.
- [x] M4 — SQS/LocalStack queue backend + worker service (consume, defer early
      messages, validate authoritative state, execute handlers, finalize outcomes,
      retry w/ backoff, dead-letter routing). Retries/backoff/DLQ ended up landing in
      this milestone rather than M5 since the worker couldn't be tested meaningfully
      without them — reconciliation and leases/heartbeats remain for M5.
- [ ] M5 — Reconciliation service (recover stuck DISPATCH_PENDING/RUNNING jobs via
      lease_expires_at), worker heartbeats, crash-recovery tests, cancellation/
      reschedule-vs-stale-message interaction tests.
- [ ] M6 — Observability: Prometheus metrics in all services, structured JSON logs w/
      correlation+job IDs, Grafana dashboard, docker-compose wiring for
      prometheus/grafana/localstack.
- [ ] M7 — Benchmarks: load generator, lateness percentile measurement, DB/queue
      metrics capture, BENCHMARKS.md with real results.
- [ ] M8 — Docs: ARCHITECTURE.md, DESIGN_DECISIONS.md, FAILURE_MODES.md, README.md,
      diagrams, AWS deployment docs (infra/aws), troubleshooting guide.
- [ ] M9 — Full test suite pass, final polish.

## Status

Repo initialized (git init, local only, no remote). Directory structure created.
Docker available locally via Colima (`docker-compose` v5.3.1 standalone binary).
No node/psql on host — everything runs inside Docker containers.

**M1 complete.** Working end-to-end via `docker-compose up`:
- FastAPI app (`app/api/`) with POST/GET/list/cancel/reschedule/attempts endpoints,
  `/health`, `/ready`, `/metrics`, `/handlers`.
- Postgres schema (jobs, outbox, attempts) via Alembic, with the indexes the
  dispatcher will need (status+scheduled_at, status+lease_expires_at,
  status+dispatched_at) already in place even though the dispatcher doesn't exist yet.
- Job state machine (`app/models/states.py`) with explicit legal-transition table,
  cancellable/reschedulable state sets.
- Idempotency-key handling with conflict detection (same key, different body -> 409).
- Cursor-based pagination for `GET /jobs`.
- 3 safe sample handlers (notify, process_data, always_fail) behind a name-based
  registry — no arbitrary code execution from job payloads.
- Structured logging (structlog) + Prometheus metrics scaffolding.
- 52 tests passing (unit + integration against real Postgres in docker-compose),
  verified by actually running them, not assumed.

Bugs caught and fixed during this milestone (see git log for detail):
- `simulate_notification` used a truthy default ("unknown") for a required field,
  which silently defeated its own validation.
- Starlette's `BaseHTTPMiddleware` (`@app.middleware("http")`) is incompatible with
  asyncpg/SQLAlchemy async sessions under test (raises "attached to a different
  loop") — replaced with a plain ASGI middleware class for the metrics/logging layer.
- Session-scoped async DB engine fixtures don't work with pytest-asyncio's
  per-test event loops — fixtures are function-scoped instead.

**M3 complete.** Added:
- `app/queue/base.py`: Queue abstraction (send_batch/receive/delete/change_visibility).
  Built this now rather than deferring to M4, since the outbox publisher needed a real
  interface to be properly testable — the SQS/LocalStack adapter still waits for M4.
- `app/queue/memory.py`: InMemoryQueue with FIFO delivery and real visibility-timeout
  semantics (undeleted messages get redelivered), documented as single-process-only.
- `app/scheduler/claim.py` + `dispatcher.py`: claims due jobs via
  `FOR UPDATE SKIP LOCKED` on the (status, scheduled_at) index, writes the outbox row
  in the same transaction, runs as N concurrent loops (models horizontal scaling even
  within one process). Verified with a real concurrency test: two claim transactions
  racing on the same 20 jobs via `asyncio.gather` end up with zero duplicates and
  every job's attempt_count exactly 1.
- `app/publisher/outbox.py` + `service.py`: claims PENDING outbox rows (also SKIP
  LOCKED), batch-publishes to the queue, commits PUBLISHED + the job's
  DISPATCH_PENDING -> QUEUED transition together. Retries up to
  `outbox_max_publish_attempts` before giving up (row -> FAILED, job stays stuck at
  DISPATCH_PENDING for reconciliation to find later in M5).
- Explicit test proving the outbox's core resilience property: a job claimed by the
  dispatcher but left unpublished for an arbitrary delay (simulating publisher
  downtime) loses nothing — the row is durable DB state, not in-memory state.
- Verified live in docker-compose: created a job via the API, watched dispatcher +
  outbox-publisher containers move it SCHEDULED -> DISPATCH_PENDING -> QUEUED with no
  worker involved yet.
- 68 tests passing (16 new), ruff clean.

Design note worth remembering for M4: `dispatch_horizon_seconds` defaults to 120s, so
jobs enter DISPATCH_PENDING/QUEUED up to 2 minutes before their actual scheduled_at —
this is intentional (SQS's 15-min delay cap means we can't just delay the message to
the exact time). The precision guarantee (99% within 2s) has to come from the
**worker**, not the dispatcher: a worker that receives a message early must defer
execution (via change_visibility) rather than running the handler immediately. This
is the next thing to build.

**M4 complete.** Added:
- `app/queue/sqs.py`: SQS adapter (boto3, sync calls offloaded via `asyncio.to_thread`)
  implementing the same Queue interface as InMemoryQueue — works against LocalStack
  locally and real AWS in production, selected purely by AWS_ENDPOINT_URL. Credentials
  always via boto3's standard provider chain, never hardcoded.
- `app/workers/claim.py`: atomic QUEUED -> RUNNING claim with the staleness checks
  that make at-least-once delivery safe (cancelled job, stale attempt_count after a
  reschedule, already-RUNNING or already-terminal duplicate delivery).
- `app/workers/executor.py`: early-arrival deferral via `change_visibility` (no
  busy-waiting — a message that arrives before its job's scheduled_at just gets
  hidden until close to due time), handler execution with a per-job timeout, and
  routing to finalize_success/finalize_permanent_failure/finalize_retryable_failure.
- `app/workers/outcomes.py`: success/permanent-failure/retry-or-dead-letter DB
  transitions. Retry path genuinely commits RUNNING -> RETRY_PENDING before bumping
  to SCHEDULED with the next backoff time, so RETRY_PENDING is actually observable,
  not just a name in an enum. Dead-letter path publishes a real message to the DLQ.
- `app/workers/retry.py`: full-jitter exponential backoff (AWS's recommended
  strategy — spreads retries instead of letting them re-collide).
- docker-compose: added `localstack` + `init-queues` (bootstraps the main+DLQ SQS
  queues) + `worker` services. Switched dispatcher/outbox-publisher/worker to
  QUEUE_BACKEND=sqs against LocalStack; API stays on the in-memory backend since it
  never touches the queue.
- **Verified live, across real separate containers talking over real SQS
  (LocalStack), not mocked**: created a job via curl, watched it flow
  SCHEDULED -> DISPATCH_PENDING -> QUEUED -> RUNNING -> SUCCEEDED across the API,
  dispatcher, outbox-publisher, and worker containers in ~1 second. Separately
  verified a permanently-retrying job reaches DEAD_LETTER and a real message lands
  in the LocalStack DLQ (`ApproximateNumberOfMessages: 1`, checked via `awslocal`).
- 89 tests passing (21 new), ruff clean.

Bug caught during this milestone: an identity-map staleness trap in test code (not
production code) — a test's `db_session` cached the `Job` ORM object it created, and
re-querying through that *same* session returned the stale cached object instead of
reflecting an update committed by a different session (the worker's own
`session_factory()`). Fixed by using `await session.refresh(job)` instead of
re-running `select()` on a session that already has the object loaded. Worth
remembering for any future test that creates state in one session and expects to
observe another component's writes to that same row.

## Current work

Starting M5: reconciliation service. The worker already sets `lease_expires_at` on
RUNNING and the outbox publisher already has a bounded retry budget, but nothing yet
*sweeps* for: jobs stuck DISPATCH_PENDING with no progress, jobs stuck RUNNING past
their lease (worker crashed mid-execution), or outbox rows stuck FAILED. That sweep
is the reconciliation service.

## Blockers

None yet.

## Notes

- Target throughput (10k jobs/sec) and 99% <2s lateness are engineering targets to
  benchmark against, not guaranteed/claimed results. BENCHMARKS.md will report
  actual measured numbers only.
- No AWS deployment or paid resources without explicit user approval.
