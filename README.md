# Chronos — High-Throughput Distributed Job Scheduler

A distributed job scheduling system built to demonstrate real backend engineering:
durable scheduling backed by Postgres, a transactional outbox for safe delivery to
SQS, horizontally-scalable dispatch/publish/execute/reconcile stages, retries with
backoff and dead-lettering, and a load-testing suite that reports actual measured
numbers rather than claims.

**Engineering targets** (10,000 jobs/sec ingestion, 99% of jobs starting within 2s
of schedule) are targets this system was *designed* for — not claims about what it
*achieves*. See [BENCHMARKS.md](BENCHMARKS.md) for real measured results: the
light-load SLA target is met (98.68%); at higher load, a concrete bottleneck was
found, profiled, and documented, with a clear path forward that doesn't require
architectural changes.

## Quick start

Requires Docker. Everything else runs in containers.

```bash
cp .env.example .env   # defaults work as-is for local dev
docker-compose up -d
```

This starts: Postgres, LocalStack (SQS emulation), the API, dispatcher,
outbox-publisher, worker, reconciliation, Prometheus, and Grafana.

```bash
curl http://localhost:8000/health        # {"status": "ok"}
curl http://localhost:8000/ready         # checks DB connectivity
open http://localhost:3000               # Grafana (admin/admin, or anonymous viewer)
open http://localhost:9090               # Prometheus
```

Create and watch a job run:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "demo.notify",
    "payload": {"recipient": "you@example.com", "message": "hello"},
    "scheduled_at": "2026-01-01T00:00:00Z"
  }'
# -> {"id": "...", "status": "SCHEDULED", ...}

curl http://localhost:8000/jobs/<id>
# watch status move SCHEDULED -> DISPATCH_PENDING -> QUEUED -> RUNNING -> SUCCEEDED
```

## Architecture

Five services, one shared Postgres database, one SQS-compatible queue:

```
API → Postgres (jobs + outbox, one transaction) → Outbox Publisher → SQS → Worker
                                                                        ↓
                                                                  Reconciliation
                                                            (sweeps stuck jobs)
```

Full component breakdown, data flow diagram, and job-lifecycle state diagram:
**[ARCHITECTURE.md](ARCHITECTURE.md)**.

Why things are built the way they are (outbox over dual-write, `SKIP LOCKED` over a
distributed lock, standard over FIFO SQS, at-least-once over a false
exactly-once promise, and more): **[DESIGN_DECISIONS.md](DESIGN_DECISIONS.md)**.

What happens when something crashes, duplicates, or arrives late — and what's
actually tested or live-verified for each: **[FAILURE_MODES.md](FAILURE_MODES.md)**.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Create a job (supports `idempotency_key`) |
| `GET` | `/jobs/{id}` | Fetch one job |
| `GET` | `/jobs` | List, filtered by `status`/`task_type`, cursor-paginated |
| `POST` | `/jobs/{id}/cancel` | Cancel (not supported once `RUNNING` — see FAILURE_MODES.md §12) |
| `POST` | `/jobs/{id}/reschedule` | Reschedule to a new `scheduled_at` |
| `GET` | `/jobs/{id}/attempts` | Full execution/retry history |
| `GET` | `/handlers` | Registered `task_type` values |
| `GET` | `/health` / `/ready` / `/metrics` | Liveness / readiness / Prometheus |

Interactive docs (Swagger UI) at `http://localhost:8000/docs` once running.

```bash
# Idempotent create — same key + same body returns the same job, no duplicate
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "task_type": "demo.process_data",
  "payload": {"items": [1, 2, 3]},
  "scheduled_at": "2026-06-01T12:00:00Z",
  "idempotency_key": "order-42-reminder"
}'

# List pending jobs, 10 at a time
curl "http://localhost:8000/jobs?status=SCHEDULED&limit=10"

# Cancel
curl -X POST http://localhost:8000/jobs/<id>/cancel

# Reschedule
curl -X POST http://localhost:8000/jobs/<id>/reschedule \
  -H "Content-Type: application/json" -d '{"scheduled_at": "2026-06-02T00:00:00Z"}'
```

### Registered task types (safe sample handlers)

No job payload executes arbitrary code — `task_type` selects one of a fixed,
reviewed set of handlers (`app/handlers/`):

- `demo.notify` — simulates sending a notification (no real network call)
- `demo.process_data` — a lightweight deterministic transform over an `items` list
- `demo.always_fail` — intentionally fails every attempt, for exercising retries/DLQ
- `bench.task` — sleeps for a caller-specified duration, used only by the benchmark suite

## Testing

```bash
docker-compose up -d postgres migrate
docker-compose run --rm -e TEST_DATABASE_URL=postgresql+asyncpg://chronos:chronos@postgres:5432/chronos_test \
  api pytest -v
```

97 tests: unit (state machine, retry backoff, handlers, schema validation),
integration (dispatcher concurrency via a real `asyncio.gather` race, outbox
crash-recovery, worker claiming/staleness checks, reconciliation sweeps — all
against a real Postgres database), and one end-to-end test driving the full
pipeline (API → dispatcher → outbox → worker) in a single test.

```bash
docker-compose run --rm api ruff check .   # lint
```

## Benchmarks

```bash
docker-compose up -d
bash benchmarks/run_benchmark.sh my_run 20 150 0   # name, duration_s, rate, burst
cat benchmarks/results/my_run/pipeline_report.json
```

Real results from three load scenarios, honest bottleneck analysis, and a concrete
(architecturally-consistent, no-rewrite-needed) path toward the throughput target:
**[BENCHMARKS.md](BENCHMARKS.md)**.

## Observability

- **Grafana** (`localhost:3000`): a 12-panel dashboard auto-provisioned on startup —
  job outcomes, dispatch lag, outbox backlog, queue backlog, worker activity,
  reconciliation recoveries, task duration, HTTP latency, DB pool usage.
- **Prometheus** (`localhost:9090`): scrapes all five services directly.
- **Structured logs**: JSON in production (`LOG_FORMAT=json`), readable console
  output locally. Every API request carries a `request_id` (from `X-Request-ID` or
  a generated UUID, echoed back as a response header); every worker message
  carries `job_id`/`attempt_number` — both via `structlog` contextvars, so every
  log line during that request/message automatically includes them with no
  explicit parameter-threading.

## Deployment

Local: `docker-compose up -d` (above). AWS: Terraform under `infra/aws/terraform/`
provisions the same five-service architecture on ECS Fargate + RDS + SQS +
Secrets Manager + CloudWatch. **Documentation and IaC only — nothing has been
applied; no AWS resources exist from this repository.** Architecture diagram, setup
steps, a real cost estimate, and teardown instructions:
**[infra/aws/DEPLOYMENT.md](infra/aws/DEPLOYMENT.md)**.

## Project structure

```
app/
  api/            FastAPI app, routes, Pydantic schemas
  scheduler/      Dispatcher (claim + outbox write)
  publisher/      Outbox publisher (outbox -> queue)
  workers/        Worker (claim + execute + finalize)
  reconciliation/ Crash-recovery sweeps
  handlers/       Registered task handlers + the registry
  queue/          Queue abstraction (SQS adapter + in-memory fake)
  db/             Session management, repository layer
  models/         SQLAlchemy models, job state machine
  config/         Settings (env-driven)
  observability/  Logging, metrics, metrics HTTP server
migrations/       Alembic
tests/
  unit/           No DB required
  integration/    Real Postgres (docker-compose's chronos_test database)
  e2e/            Full pipeline, one test
benchmarks/       Load generator, pipeline analysis, resource monitoring
dashboards/       Grafana dashboard JSON
infra/
  prometheus/     Scrape config
  grafana/        Datasource + dashboard provisioning
  aws/            Terraform + deployment docs
docs/             Troubleshooting guide, future improvements list
scripts/          One-off ops scripts (SQS queue bootstrap)
```

## What's implemented vs optional vs AWS-only

**Implemented and tested**: everything in the component table above, the full job
state machine, idempotency keys, retries with backoff+jitter, dead-lettering,
crash recovery (dispatcher/publisher/worker), reconciliation, cursor pagination,
Prometheus metrics on every service, structured logs with correlation IDs, and the
benchmark suite.

**Local-only, not production-representative**: the `QUEUE_BACKEND=memory` option
(single-process fake queue — useful for fast unit tests, not a substitute for
SQS/LocalStack in any multi-container scenario) and LocalStack itself (a SQS
emulation, not real AWS — see `BENCHMARKS.md` §2 for why this matters for the
throughput numbers specifically).

**Requires AWS credentials / costs money**: everything under `infra/aws/` — see
`infra/aws/DEPLOYMENT.md` for the explicit approval-required deployment steps and
cost estimate. Nothing in this repository has deployed anything to AWS.

## Troubleshooting and future improvements

Full troubleshooting guide: **[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**
(local dev, tests, benchmarks, AWS deployment).

Punch list derived from actual findings in this project (not a wishlist):
**[docs/FUTURE_IMPROVEMENTS.md](docs/FUTURE_IMPROVEMENTS.md)**.
