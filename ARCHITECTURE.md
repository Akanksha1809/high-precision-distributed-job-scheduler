# Chronos — Architecture

## Components and responsibilities

| Component | Responsibility | Touches |
|---|---|---|
| **API** (`app/api/`) | REST interface: create/inspect/list/cancel/reschedule jobs, attempt history, health/ready/metrics | Postgres only — never the queue |
| **Dispatcher** (`app/scheduler/`) | Finds jobs approaching their scheduled time, claims them (`SCHEDULED → DISPATCH_PENDING`), writes an outbox row — atomically | Postgres only |
| **Outbox Publisher** (`app/publisher/`) | Reads pending outbox rows, publishes to the queue, marks them published and the job `QUEUED` | Postgres + Queue |
| **Worker** (`app/workers/`) | Consumes queue messages, validates authoritative job state, executes the registered handler, records the outcome | Postgres + Queue + the handler registry |
| **Reconciliation** (`app/reconciliation/`) | Periodic sweep recovering jobs stuck `DISPATCH_PENDING` (outbox exhausted) or `RUNNING` (worker crashed) | Postgres + Queue (DLQ only) |
| **Queue** (`app/queue/`) | Abstraction over SQS/LocalStack (`sqs.py`) and an in-memory fake (`memory.py`) — everything above talks to this interface, never to boto3 directly | — |

Postgres is the single source of truth for every job's state. The queue is a
delivery mechanism, not a record of truth — a message can be delayed, duplicated,
or lost, and the system is designed to survive all three without corrupting job
state (see `FAILURE_MODES.md`).

## Request flow

```mermaid
flowchart LR
    client[Client] -->|POST /jobs| api[API]
    api -->|INSERT, status=SCHEDULED| db[(Postgres)]

    dispatcher[Dispatcher] -->|"SELECT ... FOR UPDATE SKIP LOCKED<br/>WHERE status=SCHEDULED<br/>AND scheduled_at <= now()+horizon"| db
    dispatcher -->|"same transaction:<br/>UPDATE status=DISPATCH_PENDING<br/>INSERT outbox row"| db

    publisher[Outbox Publisher] -->|"SELECT ... FOR UPDATE SKIP LOCKED<br/>WHERE outbox.status=PENDING"| db
    publisher -->|send_message_batch| queue[[SQS / LocalStack]]
    publisher -->|"UPDATE outbox=PUBLISHED,<br/>job=QUEUED"| db

    worker[Worker] -->|receive_message| queue
    worker -->|"re-check job.status,<br/>job.attempt_count,<br/>job.scheduled_at"| db
    worker -->|"UPDATE status=RUNNING"| db
    worker -->|execute handler| handler[Registered handler]
    worker -->|"UPDATE status=SUCCEEDED/<br/>FAILED/RETRY_PENDING/<br/>DEAD_LETTER"| db
    worker -->|delete_message| queue
    worker -.->|on exhausted retries| dlq[[DLQ]]

    reconciliation[Reconciliation] -->|"sweep stuck jobs"| db
    reconciliation -.->|publish on dead-letter| dlq
```

## Why the dispatcher and publisher are separate processes

This is the one design decision worth explaining up front, because it's the answer
to "how do you handle the dual-write problem between Postgres and SQS": **they
aren't actually two independent writes**. The dispatcher's claim and its outbox
write happen in one Postgres transaction — committed together or not at all. The
publisher is a separate process specifically so that a crash between "job claimed"
and "message actually sent to SQS" can never lose the job: the outbox row is
durable, committed Postgres state, not something held in a dispatcher process's
memory. If the publisher process dies mid-batch, the row is still `PENDING` and
gets picked up by any publisher replica on its next poll — see `FAILURE_MODES.md`
for the test that proves this (`test_outbox_survives_publisher_outage_with_no_data_loss`).

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> SCHEDULED: POST /jobs
    SCHEDULED --> DISPATCH_PENDING: dispatcher claims<br/>(within horizon)
    SCHEDULED --> CANCELLED: POST /cancel

    DISPATCH_PENDING --> QUEUED: outbox publisher<br/>sends to queue
    DISPATCH_PENDING --> SCHEDULED: reconciliation recovers<br/>(outbox exhausted retries)
    DISPATCH_PENDING --> CANCELLED: POST /cancel

    QUEUED --> RUNNING: worker claims<br/>(status+attempt check passes)
    QUEUED --> SCHEDULED: stale/duplicate message<br/>discarded, no-op
    QUEUED --> CANCELLED: POST /cancel

    RUNNING --> SUCCEEDED: handler returns
    RUNNING --> RETRY_PENDING: handler raises<br/>RetryableError/timeout,<br/>budget remains
    RUNNING --> FAILED: handler raises<br/>PermanentError
    RUNNING --> DEAD_LETTER: retries exhausted<br/>(or reconciliation recovers<br/>an abandoned RUNNING job<br/>past its budget)

    RETRY_PENDING --> SCHEDULED: backoff computed,<br/>scheduled_at bumped
    RETRY_PENDING --> CANCELLED: POST /cancel

    SUCCEEDED --> [*]
    FAILED --> [*]
    DEAD_LETTER --> [*]
    CANCELLED --> [*]
```

Note what's *not* a legal transition: `RUNNING → CANCELLED`. A job actively
executing can't be safely interrupted mid-handler, so cancellation is best-effort —
see `app/models/states.py` and `FAILURE_MODES.md` for why this is documented
explicitly rather than silently attempted.

Retries deliberately reuse the exact same pipeline as a fresh job
(`RETRY_PENDING → SCHEDULED`, then the normal dispatch/queue/execute cycle) instead
of a parallel "retry path" — one pipeline to test and reason about, not two that can
drift out of sync. See `app/models/states.py`'s module docstring.

## Data model

Three tables (`app/models/`), one Alembic migration:

- **`jobs`** — the authoritative record. Indexed on `(status, scheduled_at)` for the
  dispatcher's horizon query, `(status, lease_expires_at)` for reconciliation's
  abandoned-RUNNING sweep, `(status, dispatched_at)` for its stuck-DISPATCH_PENDING
  sweep, and `created_at` for list pagination.
- **`outbox`** — one row per dispatch attempt, written atomically with the job's
  `SCHEDULED → DISPATCH_PENDING` transition. Indexed on `(status, created_at)` for
  the publisher's claim query.
- **`attempts`** — append-only execution history, one row per attempt, so
  `GET /jobs/{id}/attempts` shows the full retry trail (worker, timing, error) not
  just the latest state.

See `DESIGN_DECISIONS.md` for why status columns are `VARCHAR` + `CHECK` constraint
rather than a native Postgres `ENUM`, and other schema trade-offs.
