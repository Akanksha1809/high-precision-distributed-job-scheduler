# Chronos — TODO / Progress Log

## Milestones

- [x] M1 — Vertical slice: repo scaffold, config, DB models, Alembic migration, FastAPI
      `POST /jobs` + `GET /jobs/{id}` + `GET /health`, job state machine, unit tests.
- [ ] M2 — Full job CRUD: list/filter/paginate, cancel, reschedule, attempts history,
      idempotency keys, integration tests against real Postgres (docker compose).
- [ ] M3 — Dispatcher + transactional outbox (claim via FOR UPDATE SKIP LOCKED,
      outbox table, no queue yet — just marks QUEUED and logs).
- [ ] M4 — Queue abstraction (interface) + in-memory impl (tests) + SQS/LocalStack impl.
      Outbox publisher service wired to real queue. Worker service consumes + executes
      registered handlers, re-validates state before running.
- [ ] M5 — Retries w/ exponential backoff+jitter, DLQ routing, reconciliation service,
      worker leases/heartbeats, crash recovery tests, duplicate-delivery tests.
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

## Current work

Starting M2: full job CRUD polish is mostly already done as part of M1's vertical
slice (list/filter/paginate/cancel/reschedule/attempts/idempotency all exist and are
tested). Re-scoping M2 to: review for gaps, then move to M3 (dispatcher + outbox).

## Blockers

None yet. Noting for later: LocalStack image pulls may hit the same transient
registry timeout seen with postgres:16-alpine during M1 — retry if it happens.

## Notes

- Target throughput (10k jobs/sec) and 99% <2s lateness are engineering targets to
  benchmark against, not guaranteed/claimed results. BENCHMARKS.md will report
  actual measured numbers only.
- No AWS deployment or paid resources without explicit user approval.
