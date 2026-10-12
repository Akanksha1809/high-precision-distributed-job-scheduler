# Design Decisions and Trade-offs

This document explains *why*, not *what* — the code and `ARCHITECTURE.md` cover
what exists. This covers the choices that had real alternatives, and why this one
was picked.

## At-least-once delivery, not exactly-once

**This system does not provide exactly-once execution, and no honest distributed
system built on SQS (or any standard message queue) can.** SQS standard queues are
at-least-once by design — a message can be delivered more than once, and can arrive
out of order. Rather than pretending otherwise, Chronos is built around that reality:

- Every worker re-validates the job's authoritative state (status, `attempt_count`)
  against Postgres *before* executing, discarding anything stale (cancelled,
  superseded by a reschedule, already terminal) — see `app/workers/claim.py`.
- Handlers are expected to be **idempotent** for any side effect that matters. The
  system guarantees a handler is never invoked concurrently for the same job+attempt
  (enforced by the atomic `QUEUED → RUNNING` claim), and guarantees it's never
  invoked again *after* that attempt reaches a terminal state — but if a handler
  itself has a non-idempotent side effect (e.g. charging a credit card) and the
  worker process crashes *after* the side effect but *before* committing the
  outcome, that side effect could in principle happen again on retry. This is an
  inherent limitation of at-least-once execution with external side effects, not a
  bug — the standard mitigation is to make the external call itself idempotent
  (e.g. an idempotency key passed to a payment API), which is the handler author's
  responsibility, not the scheduler's. The three sample handlers in
  `app/handlers/samples.py` are simple enough not to need this, but a production
  handler for anything with a real external side effect should follow this pattern.

This is the practical baseline every production job scheduler built on a standard
queue actually uses (SQS, Celery with most brokers, Sidekiq, etc.) — the honest
answer to "what if a job runs twice" is "it shouldn't, and if it does, the handler
needs to tolerate it," not "it's architecturally impossible."

## Transactional outbox over a direct dual-write

**The problem**: moving a job from `SCHEDULED` to "dispatched" requires two
separate systems to agree — Postgres (the record) and SQS (the delivery). A naive
"update Postgres, then call SQS" has an unavoidable gap: if the process crashes
between the two, you either lost the dispatch (never called SQS) or you don't know
whether you did (crashed right as the SQS call was in flight).

**The choice**: write an outbox row in the *same Postgres transaction* as the status
change, then let a separate process publish outbox rows to SQS asynchronously. This
converts "two systems, one transaction" (impossible) into "one system, one
transaction, plus a separately-retriable async step" (straightforward). If the
publisher crashes mid-batch, the outbox row is still `PENDING` — durable committed
state, not something held in a process's memory — and any publisher replica picks
it up on its next poll. Proven directly in
`tests/integration/test_outbox_publisher.py::test_outbox_survives_publisher_outage_with_no_data_loss`.

**Trade-off accepted**: an extra table, an extra process, and a small amount of
publish latency (bounded by `outbox_poll_interval_ms`, default 100ms) between a job
being claimed and the message actually reaching SQS. This is the standard,
well-documented pattern for exactly this problem (see Chris Richardson's
microservices.io outbox pattern) — the alternative (two-phase commit across
Postgres and SQS) isn't something SQS supports at all.

## `FOR UPDATE SKIP LOCKED` over a distributed lock

Claiming jobs for dispatch, and claiming outbox rows for publishing, both need
"exactly one of N concurrent replicas gets this row" semantics. Options considered:

- **A distributed lock** (Redis, Postgres advisory locks at the table level) — adds
  another moving part, and advisory locks at that granularity would serialize
  dispatch across the whole table rather than per-row.
- **`SELECT ... FOR UPDATE SKIP LOCKED`** (chosen) — native Postgres row-level
  locking. Two dispatchers racing on an overlapping batch window simply end up with
  disjoint claimed sets; neither blocks the other, and there's no separate lock
  service to run or reason about. Proven directly with a real concurrency test
  (`test_concurrent_dispatchers_never_double_claim`): two claim transactions race via
  `asyncio.gather` on the same 20 jobs, and every job ends up claimed exactly once.

This is what makes the dispatcher, publisher, and reconciliation sweep all
horizontally scalable with zero coordination code.

## Horizon-based dispatch, not one SQS delay per job

SQS's `DelaySeconds` caps at 15 minutes — you cannot schedule a message for
"tomorrow" via a single delayed send. The dispatcher instead works within a
rolling horizon (`dispatch_horizon_seconds`, default 120s): a job only enters the
dispatch pipeline once it's within that window of its `scheduled_at`, regardless of
whether it was created a second ago or a month ago. Far-future jobs just sit as
`SCHEDULED` rows — the `(status, scheduled_at)` index means they cost nothing to
skip over; the dispatcher's query never touches them until they're actually within
horizon.

This means jobs can be dispatched (and reach `QUEUED`) up to `dispatch_horizon_
seconds` *before* their actual scheduled time. The precision guarantee (99% within
2s) therefore cannot come from the dispatcher — it has to come from the **worker**,
which is why `app/workers/executor.py` defers any message that arrives before its
job's `scheduled_at` via `change_visibility` rather than executing early or
busy-waiting.

## Standard SQS queue, not FIFO

FIFO queues cap at roughly 3,000 messages/sec with batching — well below this
project's throughput target — in exchange for strict ordering and
exactly-once-delivery guarantees within a message group. Chronos doesn't need
either guarantee: jobs are independent of each other (no ordering requirement
between unrelated jobs), and the worker's own state-check-before-execute logic
already handles duplicate delivery safely. Standard queues' higher throughput
ceiling and simpler operational model (no message-group-ID management) made this
an easy choice once the ordering/dedup guarantees were confirmed unnecessary.

## Application-level DLQ routing, not purely SQS's native redrive

The job's `max_retries` is set per-job (via the API, defaulting to a global
setting) and tracked in Postgres as `attempt_count` vs `max_retries` — this is the
primary mechanism deciding when a job is dead-lettered
(`app/workers/outcomes.py::finalize_retryable_failure`). SQS's own
`RedrivePolicy`/`maxReceiveCount` (configured in `infra/aws/terraform/sqs.tf` with
a deliberately high threshold of 20) is a secondary backstop for a scenario the
app-level logic can't catch: a bug that crashes the worker before it even reaches
the retry-counting code, every single time. Relying on SQS's redrive as the
*primary* mechanism wouldn't work locally at all — the in-memory queue
(`app/queue/memory.py`), used in tests and `QUEUE_BACKEND=memory` local dev, has no
concept of a redrive policy, and building one just for test parity wasn't worth it
when the real logic needed to live in the application anyway (it needs
`max_retries`, which is a per-job value SQS has no way to know).

## `VARCHAR` + `CHECK` constraint over a native Postgres `ENUM` for status columns

Postgres native enums (`CREATE TYPE ... AS ENUM`) are slightly more storage-
efficient and self-documenting in `\d` output, but altering them (adding a new
value) requires `ALTER TYPE ... ADD VALUE`, which historically couldn't run inside
a transaction block in older Postgres versions and is still an operation with sharp
edges around concurrent use. A `VARCHAR(32)` with a `CHECK` constraint
(`app/models/job.py`, `app/models/outbox.py`, `app/models/attempt.py`) is one more
migration to add/remove a value from the `CHECK`, with none of the lock-contention
caveats, at the cost of 32 bytes instead of 4 per row — a trade worth making for a
state machine that's realistically going to evolve during development (it did —
`DEAD_LETTER` was added as a state distinct from `FAILED` partway through building
this, see `app/models/states.py`'s module docstring).

## Idempotency keys with a request-body hash, not just the key alone

`POST /jobs` accepts an optional `idempotency_key`. Storing just the key and
returning the existing job on a repeat would silently paper over a client bug where
the same key is reused with a *different* request body (different task_type,
payload, or scheduled_at) — the client would think it created job B but actually
got job A back, with no indication anything was wrong. Chronos instead stores a
hash of the canonical request body alongside the key
(`app/db/repository.py::_request_hash`) and returns `409 Conflict` if the same key
is replayed with a different body — a deliberately louder failure mode than
silently returning the wrong job.

## Cursor-based pagination, not offset/limit

`GET /jobs` pages by an opaque `(created_at, id)` cursor rather than
`?offset=N&limit=M`. Offset pagination re-scans and discards the first N rows on
every page (expensive at scale) and gives inconsistent results if rows are inserted
between page fetches (page 2 can show rows already seen on page 1, or skip rows
entirely). The cursor approach uses the same `created_at` index the list query
already needs, costs nothing extra, and stays stable as new jobs are created
concurrently with someone paging through results — proven by
`test_list_jobs_pagination`'s explicit check that page 1 and page 2 never overlap.

## boto3 (sync) + `asyncio.to_thread`, not an async-native SQS client

`app/queue/sqs.py` wraps every boto3 call in `asyncio.to_thread` rather than using
an async-native library like `aioboto3`. boto3 is the actively-maintained, canonical
AWS SDK for Python; `aioboto3` is a wrapper *around* boto3 (not a reimplementation),
adding a second dependency surface and its own compatibility lag against boto3
releases, for a benefit that only matters at a concurrency level well beyond what a
single worker/publisher process needs here (each `to_thread` call is a single
blocking network request, not a CPU-bound operation — the thread pool handles this
fine at the concurrency levels `outbox_publisher_concurrency` and
`worker_concurrency` default to).

## A plain ASGI middleware, not Starlette's `BaseHTTPMiddleware`

Found directly, not anticipated: `BaseHTTPMiddleware` (what `@app.middleware("http")`
uses under the hood) runs the downstream app in a separate task via anyio's task
group, which has a documented incompatibility with asyncpg/SQLAlchemy async
sessions — connections created inside the route handler would occasionally raise
"attached to a different loop" under test. `app/api/main.py`'s
`ObservabilityMiddleware` is a plain ASGI middleware class instead, which runs in
the same task as the request with no connection handoff. Caught by the integration
test suite during M1, not by reading documentation in advance — see `TODO.md`'s
M1 notes.
