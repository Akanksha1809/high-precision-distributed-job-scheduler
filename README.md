# Chronos — High-Throughput Distributed Job Scheduler

[![CI](https://github.com/Akanksha1809/high-precision-distributed-job-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/Akanksha1809/high-precision-distributed-job-scheduler/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](Dockerfile)

Chronos is a distributed job scheduler: you tell it *what* to run and *when*, and
it guarantees the job runs — exactly once from the caller's point of view, durably,
on time, and recoverably — across a fleet of stateless workers, even if any
individual process crashes mid-flight. It's built the way a real scheduling
platform would be built: Postgres as the single source of truth, a message queue
as a delivery mechanism (not a record of truth), and every failure mode — crashed
dispatcher, crashed worker, duplicate delivery, a message that arrives before it's
due — handled by design and proven with a real test or a real live run, not assumed
away.

This README covers what the project set out to do, how it actually does it, and —
because claims are cheap — real transcripts of it doing it.

## Contents

- [What this project set out to do](#what-this-project-set-out-to-do)
- [How it actually works](#how-it-actually-works)
- [Quick start](#quick-start)
- [Demos](#demos) — job lifecycle, idempotency, cancel/reschedule, retries & DLQ, crash recovery, observability, benchmarks
- [API reference](#api-reference)
- [Testing & CI](#testing--ci)
- [Deployment](#deployment)
- [Project structure](#project-structure)
- [Implementation status](#implementation-status)
- [Documentation map](#documentation-map)

## What this project set out to do

The brief for this project was explicit: build this like a real scheduling
platform, with real engineering targets to design *toward* — not to claim
achievement of without measuring.

| Goal | Status |
|---|---|
| Durable job persistence, surviving process restarts | ✅ Postgres is the only source of truth for job state — see [Demo 5](#5-crash-recovery-reconciliation-healing-a-crashed-worker) |
| Horizontal scaling of dispatchers and workers, with no double-processing | ✅ `FOR UPDATE SKIP LOCKED` claiming, proven with a real concurrency test — see [How it actually works](#how-it-actually-works) |
| Retry handling with backoff, and dead-letter queue support | ✅ Full-jitter exponential backoff, application-level DLQ routing — see [Demo 4](#4-retries-with-backoff-and-the-dead-letter-queue) |
| Safe recovery after service restarts and worker crashes | ✅ A reconciliation sweep, proven live against a simulated crash — see [Demo 5](#5-crash-recovery-reconciliation-healing-a-crashed-worker) |
| Job cancellation and rescheduling | ✅ With explicit, documented limits (a `RUNNING` job can't be safely interrupted) — see [Demo 3](#3-cancellation-and-rescheduling) |
| Metrics, dashboards, structured logs, performance reports | ✅ Prometheus + Grafana + correlation-ID logging, all running — see [Demo 6](#6-observability-prometheus--grafana) |
| **10,000 scheduled jobs/sec ingestion** | ⚠️ **Engineering target, not achieved on this hardware.** Measured ceiling ~80-100 req/s on a resource-constrained laptop/Colima setup — see [Demo 7](#7-performance-benchmarks) and [BENCHMARKS.md](BENCHMARKS.md) |
| **99% of jobs starting within 2s of schedule** | ⚠️ **Met at light load (98.68%), degrades under sustained/overload load** — same demo and doc |

The two engineering targets are reported honestly because that's the point of
benchmarking something instead of asserting it. [BENCHMARKS.md](BENCHMARKS.md)
has the full methodology, three real load scenarios, a profiled bottleneck
analysis (confirmed via live Prometheus queries, not guessed), and a concrete,
architecturally-consistent path to the target that doesn't require rewriting
anything — it requires running more than one instance of each service, which this
benchmark deliberately never did.

## How it actually works

Five independent services share one Postgres database and one SQS-compatible
queue. Postgres is the only thing that's ever authoritative about a job's state;
the queue is purely a delivery mechanism, which is why a lost, duplicated, or
early message can never corrupt anything — every consumer re-checks Postgres
before acting.

```
API ──▶ Postgres (jobs + outbox, written in ONE transaction)
              │
              ▼ dispatcher claims via FOR UPDATE SKIP LOCKED
      Dispatcher ──▶ Outbox Publisher ──▶ SQS (/LocalStack) ──▶ Worker
                                                                    │
                                                        on exhausted retries
                                                                    ▼
                                                                   DLQ
                                                                    ▲
                                              Reconciliation ───────┘
                                        (sweeps stuck jobs, reuses the
                                           same retry/DLQ logic above)
```

| Service | Responsibility |
|---|---|
| **API** | REST interface — create/inspect/list/cancel/reschedule jobs. Only ever touches Postgres, never the queue. |
| **Dispatcher** | Finds jobs approaching their scheduled time, claims them, writes an outbox row — atomically, in one transaction. |
| **Outbox Publisher** | Reads pending outbox rows, publishes to SQS, marks the job `QUEUED` once the send actually succeeds. |
| **Worker** | Consumes queue messages, re-validates the job's real state in Postgres, executes the registered handler, records the outcome. |
| **Reconciliation** | Periodic sweep that recovers jobs stuck mid-dispatch or abandoned mid-execution — the thing that makes crash recovery automatic instead of manual. |

**Why a separate Dispatcher and Outbox Publisher, instead of just calling SQS
directly from the API or the dispatcher?** This is the answer to the classic
dual-write problem — how do you keep Postgres and SQS in sync when a process can
crash between updating one and calling the other? The dispatcher's job claim and
its outbox-row write happen in **one Postgres transaction** — committed together
or not at all. A separate publisher process then reads that durable, committed
outbox row and sends it to SQS asynchronously. If the publisher crashes mid-send,
the row is still sitting there as `PENDING` — not lost in a dead process's memory
— and any publisher replica picks it up on the next poll. [Demo 5](#5-crash-recovery-reconciliation-healing-a-crashed-worker)
proves this isn't just a diagram.

**Why `FOR UPDATE SKIP LOCKED` instead of a distributed lock (Redis, etc.)?**
Because Postgres already has row-level locking built in, and it's exactly the
right granularity: two dispatcher replicas racing on the same batch of due jobs
simply end up claiming disjoint sets — neither blocks the other, and there's no
extra service to run. This was proven directly, not assumed: a test races two
claim transactions via `asyncio.gather` against the same 20 jobs and asserts every
single one was claimed exactly once, by exactly one dispatcher.

**Why does the worker re-check everything before executing?** Because SQS is
at-least-once by contract, not exactly-once — a message can arrive twice, or
arrive before its job is actually due (the dispatcher works within a 120-second
horizon ahead of each job's scheduled time, since SQS can't delay a message by
more than 15 minutes). The worker atomically claims `QUEUED → RUNNING` only if the
job's current state and attempt number still match the message — anything stale
(cancelled, rescheduled since, already finished, or simply early) is discarded
instead of executed. [Demo 4](#4-retries-with-backoff-and-the-dead-letter-queue)
and [Demo 5](#5-crash-recovery-reconciliation-healing-a-crashed-worker) exercise
this directly.

A full component breakdown, native Mermaid diagrams (request flow + the complete
job lifecycle state machine), and every schema/table decision:
**[ARCHITECTURE.md](ARCHITECTURE.md)**. Every design trade-off explained with the
alternative considered and why it lost: **[DESIGN_DECISIONS.md](DESIGN_DECISIONS.md)**.
Every failure scenario, each tied to the specific test or live run that proves
it's handled: **[FAILURE_MODES.md](FAILURE_MODES.md)**.

## Quick start

Requires Docker. Everything else runs in containers — no local Python/Postgres
install needed.

```bash
git clone https://github.com/Akanksha1809/high-precision-distributed-job-scheduler.git
cd high-precision-distributed-job-scheduler
cp .env.example .env   # defaults work as-is for local dev
docker-compose up -d
```

This starts nine containers: Postgres, LocalStack (SQS emulation), the API,
dispatcher, outbox-publisher, worker, reconciliation, Prometheus, and Grafana.

```bash
curl http://localhost:8000/health   # {"status":"ok"}  — liveness
curl http://localhost:8000/ready    # {"status":"ready"} — DB connectivity check
open http://localhost:3000          # Grafana (admin/admin, or browse anonymously)
open http://localhost:9090          # Prometheus
open http://localhost:8000/docs     # Swagger UI
```

## Demos

Every transcript below is from an actual run of this system — curl against the
live API, real Postgres rows, real SQS (LocalStack) messages. Nothing here is
hypothetical output.

### 1. Job lifecycle, start to finish

```bash
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "task_type": "demo.notify",
  "payload": {"recipient": "you@example.com", "message": "hello"},
  "scheduled_at": "2020-01-01T00:00:00Z"
}'
```
```json
{"id":"5d0466f4-387c-4625-9921-28330c5cf9a4","status":"SCHEDULED", ...}
```

A couple of seconds later, with zero further action:

```bash
curl -s http://localhost:8000/jobs/5d0466f4-387c-4625-9921-28330c5cf9a4
```
```json
{
  "id": "5d0466f4-387c-4625-9921-28330c5cf9a4",
  "status": "SUCCEEDED",
  "dispatched_at": "2026-10-12T09:31:11.154016Z",
  "queued_at":     "2026-10-12T09:31:11.171547Z",
  "started_at":    "2026-10-12T09:31:11.184270Z",
  "completed_at":  "2026-10-12T09:31:11.217061Z"
}
```
`SCHEDULED → DISPATCH_PENDING → QUEUED → RUNNING → SUCCEEDED`, each timestamp
written by a different container (dispatcher, outbox-publisher, worker), coordinated
through nothing but Postgres row state and a real SQS queue in between.

### 2. Idempotent job creation

The same `idempotency_key` with the same request body returns the *same* job — no
duplicate created, safe to retry a flaky client call:

```bash
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "task_type":"demo.notify","payload":{"recipient":"alice@example.com","message":"hello"},
  "scheduled_at":"2026-10-13T00:00:00Z","idempotency_key":"test-key-1"
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])"
# -> 8d52402d-4386-4ad9-b024-b8fb1cbfb284

# Repeat the exact same request:
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "task_type":"demo.notify","payload":{"recipient":"alice@example.com","message":"hello"},
  "scheduled_at":"2026-10-13T00:00:00Z","idempotency_key":"test-key-1"
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])"
# -> 8d52402d-4386-4ad9-b024-b8fb1cbfb284   (same job, not a duplicate)
```

The same key reused with a **different** body is treated as a client bug, not
silently accepted:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" -d '{
    "task_type":"demo.notify","payload":{"recipient":"bob@example.com"},
    "scheduled_at":"2026-10-13T00:00:00Z","idempotency_key":"test-key-1"
  }'
# -> 409
```

### 3. Cancellation and rescheduling

```bash
curl -s -X POST http://localhost:8000/jobs/8d52402d-4386-4ad9-b024-b8fb1cbfb284/cancel
# -> {"status":"CANCELLED", ...}

curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/jobs/8d52402d-4386-4ad9-b024-b8fb1cbfb284/cancel
# -> 409   (already cancelled — not a silent no-op)

curl -s -X POST http://localhost:8000/jobs/8d52402d-4386-4ad9-b024-b8fb1cbfb284/reschedule \
  -H "Content-Type: application/json" -d '{"scheduled_at":"2026-10-14T00:00:00Z"}'
# -> {"status":"SCHEDULED", "scheduled_at":"2026-10-14T00:00:00Z", ...}
```
Cancelling a job that's already `RUNNING` is deliberately **not** supported —
there's no safe way to interrupt a handler mid-execution, so the API returns `409`
rather than pretending to stop it. Documented in [FAILURE_MODES.md §12](FAILURE_MODES.md).

### 4. Retries with backoff, and the dead-letter queue

A job whose handler always fails, with its retry budget capped at 1:

```bash
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "task_type":"demo.always_fail","payload":{"mode":"retryable"},
  "scheduled_at":"2020-01-01T00:00:00Z","max_retries":1
}'
```

One second later:

```bash
curl -s http://localhost:8000/jobs/73609b1b-cce3-468d-a3a3-b23f666ee5ac
```
```json
{
  "status": "DEAD_LETTER",
  "attempt_count": 1,
  "last_error": "always_fail handler: retryable failure requested",
  "dead_letter_reason": "always_fail handler: retryable failure requested"
}
```

And a real message actually landed in the dead-letter queue — not just a status
flag, checked directly against LocalStack's SQS API:

```bash
docker-compose exec -T localstack awslocal sqs get-queue-attributes \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/chronos-jobs-dlq \
  --attribute-names ApproximateNumberOfMessages
```
```json
{"Attributes": {"ApproximateNumberOfMessages": "1"}}
```

With `max_retries` set higher, the same job instead moves to `RETRY_PENDING` then
back to `SCHEDULED` with a full-jitter exponential backoff delay, and goes through
the exact same dispatch/queue/execute pipeline again for its next attempt — retries
aren't a special parallel code path, they're the same pipeline with a future
`scheduled_at`.

### 5. Crash recovery: reconciliation healing a crashed worker

This is the demo that actually matters for a "distributed" scheduler: proving a
worker process dying mid-execution doesn't lose the job, without any human
intervention.

A worker crash mid-execution can't be triggered cleanly by just killing a
container (it races with in-flight requests), so it's reproduced directly: a job
is inserted straight into the live Postgres as `RUNNING`, with a lease that's
already expired — exactly the state a real crash would leave behind — while the
dispatcher, outbox-publisher, worker, and reconciliation containers are all
running normally, over real SQS:

```bash
docker-compose exec -T api python3 -c "
import asyncio
from datetime import datetime, timedelta, timezone
from app.db.session import get_sessionmaker
from app.models.job import Job
from app.models.attempt import AttemptRecord, AttemptOutcome
from app.models.states import JobStatus

async def main():
    async with get_sessionmaker()() as session:
        now = datetime.now(timezone.utc)
        job = Job(task_type='demo.notify', payload={'recipient':'a@b.com'},
                   scheduled_at=now - timedelta(minutes=5), status=JobStatus.RUNNING.value,
                   attempt_count=1, max_retries=3, leased_by='worker-that-definitely-crashed',
                   lease_expires_at=now - timedelta(seconds=10), started_at=now - timedelta(seconds=30))
        session.add(job); await session.commit(); await session.refresh(job)
        session.add(AttemptRecord(job_id=job.id, attempt_number=1,
                     worker_id='worker-that-definitely-crashed', outcome=AttemptOutcome.RUNNING.value))
        await session.commit()
        print('inserted crashed job id:', job.id)
asyncio.run(main())
"
```
```
inserted crashed job id: 0432fa94-51e1-4077-ad6e-b03fdbd816b5
```

The job sits at `RUNNING` — genuinely stuck, exactly like a real crash — until the
reconciliation service's next sweep (every 30s by default):

```
reconciliation-1 | ... reconciliation_recovered_abandoned_running component=reconciliation
                    job_id=0432fa94-51e1-4077-ad6e-b03fdbd816b5 leased_by=worker-that-definitely-crashed
```

It gets retried through the **real** pipeline — redispatched, republished to SQS,
picked up by an actual live worker — and completes:

```bash
curl -s http://localhost:8000/jobs/0432fa94-51e1-4077-ad6e-b03fdbd816b5/attempts
```
```json
[
  {
    "attempt_number": 1,
    "worker_id": "worker-that-definitely-crashed",
    "outcome": "FAILED_RETRYABLE",
    "error_message": "worker lease expired (presumed crashed; last leased_by=worker-that-definitely-crashed)"
  },
  {
    "attempt_number": 2,
    "worker_id": "worker-1",
    "outcome": "SUCCEEDED"
  }
]
```

Attempt 1 is recorded as the system itself found and labeled it — a lease
expiry, not a handler error. Attempt 2 ran on a different, healthy worker and
succeeded. Nothing about this required a human to notice or intervene.

### 6. Observability: Prometheus & Grafana

All five services expose `/metrics`; Prometheus scrapes every one of them:

```bash
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json,sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    print(t['labels']['job'], t['health'])
"
```
```
chronos-api               up
chronos-dispatcher        up
chronos-outbox-publisher  up
chronos-reconciliation    up
chronos-worker            up
```

A 12-panel Grafana dashboard — job outcomes, dispatch lag, outbox backlog, queue
backlog, worker activity, reconciliation recoveries, task duration, HTTP latency,
DB pool usage — auto-provisions on startup, confirmed via Grafana's own API with
no manual import step:

```bash
curl -s -u admin:admin http://localhost:3000/api/search
```
```json
[{"uid": "chronos-overview", "title": "Chronos Overview", "url": "/d/chronos-overview/chronos-overview"}]
```

Every API request carries a `request_id` (from the client's `X-Request-ID` header,
or a generated UUID, echoed back in the response) bound into every log line for
that request via `structlog` contextvars; every worker message carries
`job_id`/`attempt_number` the same way — correlation that doesn't require threading
an ID through every function signature by hand.

### 7. Performance benchmarks

```bash
bash benchmarks/run_benchmark.sh my_run 20 150 0   # name, duration_s, target_rate, burst_size
cat benchmarks/results/my_run/pipeline_report.json
```

Three real scenarios were run and measured (full methodology and raw numbers in
[BENCHMARKS.md](BENCHMARKS.md)):

| Scenario | Ingestion achieved | % jobs within 2s of schedule | Failures |
|---|---|---|---|
| Light load (10 req/s target) | 9.93 req/s | **98.68%** | 0% |
| Sustained (150 req/s target) | 84-98 req/s | 11-18% | 0% |
| Overload (500 req/s + 2000 burst) | 78.8 req/s | 3.9% | 0% (client-side timeouts only) |

The light-load SLA target is met outright. Under heavier load, ingestion
throughput converges to the same ~80-100 req/s regardless of how much is
requested — a hard ceiling, confirmed via live Prometheus queries to be the API's
Postgres connection pool hitting saturation (20/20 connections) combined with a
single `uvicorn` process and 4 shared vCPUs across all nine containers on one
laptop. **At every load level tested, including severe overload: 0% failure
rate, 0% retry rate, 0 jobs started early, no server errors.** The system
degrades by getting slow under load, never by executing something wrong or
losing a job — see [BENCHMARKS.md §7](BENCHMARKS.md) for the full breakdown and
§6 for the concrete (no-rewrite-needed) scaling path.

## API reference

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

### Registered task types (safe sample handlers)

No job payload executes arbitrary code — `task_type` selects one of a fixed,
reviewed set of handlers (`app/handlers/`):

- `demo.notify` — simulates sending a notification (no real network call)
- `demo.process_data` — a lightweight deterministic transform over an `items` list
- `demo.always_fail` — intentionally fails every attempt, for exercising retries/DLQ
- `bench.task` — sleeps for a caller-specified duration, used only by the benchmark suite

## Testing & CI

```bash
docker-compose up -d postgres migrate
docker-compose run --rm -e TEST_DATABASE_URL=postgresql+asyncpg://chronos:chronos@postgres:5432/chronos_test \
  api pytest -v
```

97 tests: unit (state machine, retry backoff, handlers, schema validation),
integration (dispatcher concurrency via a real `asyncio.gather` race, outbox
crash-recovery, worker claiming/staleness checks, reconciliation sweeps — all
against a real Postgres database, not mocked), and one end-to-end test driving
the full pipeline (API → dispatcher → outbox → worker) in a single test.

```bash
docker-compose run --rm api ruff check .   # lint
```

CI (`.github/workflows/ci.yml`) runs both of the above — lint and the full pytest
suite against a real Postgres service container — on every push and pull request
to `main`. The CI job builds the same Docker images `docker-compose.yml` defines
and runs the exact command shown above, so a green badge means the documented
command actually works, not a separately-maintained CI-only script.

## Deployment

**Local**: `docker-compose up -d` (above) — the full nine-container stack,
including Prometheus/Grafana and LocalStack's SQS emulation.

**AWS**: Terraform under `infra/aws/terraform/` provisions the same
five-service architecture on ECS Fargate, backed by RDS Postgres, real SQS
(main queue + DLQ), Secrets Manager for credentials, and CloudWatch for logs and
alarms — least-privilege IAM throughout (the API's task role, for instance, has
zero SQS permissions, enforced at the IAM level, not just by convention).

**This is documentation and Infrastructure-as-Code only.** The Terraform has
been syntax-validated (`terraform validate`) but never applied — no AWS
resources have been created, no AWS credentials were used, and no cost has been
incurred. Architecture diagram, setup steps, a real cost estimate (~$55-70/mo if
left running), production-hardening notes, and teardown instructions:
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
.github/          CI workflow
```

## Implementation status

**Implemented and tested**: everything in the architecture diagram above, the
full job state machine, idempotency keys, retries with backoff+jitter,
dead-lettering, crash recovery (dispatcher/publisher/worker), reconciliation,
cursor pagination, Prometheus metrics on every service, structured logs with
correlation IDs, and the benchmark suite.

**Local-only, not production-representative**: the `QUEUE_BACKEND=memory` option
(a single-process fake queue — useful for fast unit tests, not a substitute for
SQS/LocalStack in any multi-container scenario) and LocalStack itself (a SQS
*emulation*, not real AWS — see [BENCHMARKS.md §2](BENCHMARKS.md) for why that
matters specifically for the throughput numbers).

**Requires AWS credentials / costs money**: everything under `infra/aws/` — see
[infra/aws/DEPLOYMENT.md](infra/aws/DEPLOYMENT.md) for the explicit
approval-required deployment steps and cost estimate. Nothing in this repository
has deployed anything to AWS.

## Documentation map

| Document | Covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Component responsibilities, data-flow diagram, job lifecycle state diagram, schema |
| [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) | Every trade-off, with the alternative considered and why it lost |
| [FAILURE_MODES.md](FAILURE_MODES.md) | 13 failure scenarios, each tied to a specific test or live verification |
| [BENCHMARKS.md](BENCHMARKS.md) | Full benchmark methodology, raw results, bottleneck analysis, scaling path |
| [infra/aws/DEPLOYMENT.md](infra/aws/DEPLOYMENT.md) | AWS architecture, setup, cost estimate, teardown |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Local dev, tests, benchmarks, AWS — common issues and fixes |
| [docs/FUTURE_IMPROVEMENTS.md](docs/FUTURE_IMPROVEMENTS.md) | A punch list derived from this project's own findings, not a wishlist |

## License

[MIT](LICENSE).
