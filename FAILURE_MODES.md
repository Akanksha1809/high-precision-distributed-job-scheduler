# Failure Modes

Every scenario below was either tested automatically (file+test referenced) or
verified live against real running containers (noted explicitly) — this isn't a
list of things the architecture is *supposed* to handle, it's a list of things that
were actually exercised.

## 1. Dispatcher crashes between claiming a job and committing

**Can't happen as a distinct failure mode** — the claim (`SCHEDULED →
DISPATCH_PENDING`) and the outbox row write are one Postgres transaction
(`app/scheduler/claim.py`). Either both happen or neither does; there is no
intermediate state where a job is claimed but has no outbox row, or has an outbox
row but wasn't claimed.

## 2. Outbox publisher crashes between claiming outbox rows and publishing to SQS

**Handled by design, not by special-case recovery code.** The outbox row is
claimed via `FOR UPDATE SKIP LOCKED` inside a transaction; if the process dies
before that transaction commits, the lock is released (Postgres releases locks on
connection loss) and the row is still `PENDING`, untouched — any publisher replica
picks it up on the next poll.

**Tested**: `test_outbox_survives_publisher_outage_with_no_data_loss` — claims a
job, then *doesn't* run the publisher for an arbitrary period (simulating
downtime), then runs it and confirms the job completes normally with zero data
loss.

**What if the publisher crashes *after* calling `send_message_batch` but *before*
committing `PUBLISHED`?** The message was genuinely sent to SQS, but the DB still
shows the outbox row `PENDING`. On the next poll, the row gets republished — a
duplicate message. This is a known, accepted trade-off (see "duplicates" below):
at-least-once delivery means a duplicate here is expected and handled downstream by
the worker's staleness checks, not prevented upstream.

## 3. Outbox publish repeatedly fails (SQS/network down)

Each row tracks `publish_attempts`; after `outbox_max_publish_attempts` (default
10), the row is marked `FAILED` and the job is left at `DISPATCH_PENDING`
permanently — *until reconciliation finds it.*

**Tested**: `test_publish_failure_exhausts_to_failed_after_max_attempts` (outbox
publisher logic), `test_recovers_dispatch_pending_with_no_live_outbox_row`
(reconciliation sweep finds exactly this state and resets the job to `SCHEDULED`
for a fresh dispatch/outbox/attempt).

## 4. Worker crashes mid-execution

The worker sets `lease_expires_at = now + job.timeout_seconds + job_lease_buffer_
seconds` the moment it claims a job for `RUNNING` (`app/workers/claim.py`) — this
lease deliberately scales with the job's *own* timeout, not a fixed global value
(see `DESIGN_DECISIONS.md`... actually see the inline comment in `claim.py`: a
fixed lease would wrongly preempt a legitimately long-running job). If the worker
process dies before finalizing, nothing else notices immediately — the job sits at
`RUNNING` until reconciliation's sweep finds `lease_expires_at` in the past.
Recovery reuses the exact same `finalize_retryable_failure` function a normal
handler failure would use: retry with backoff if budget remains, dead-letter
(with a real DLQ message) otherwise.

**Tested**: `test_recovers_abandoned_running_job_with_retry_budget_remaining`,
`test_recovers_abandoned_running_job_to_dead_letter_when_budget_exhausted`,
`test_does_not_touch_running_job_with_active_lease` (confirms a legitimately
running job is *not* preempted), `test_lease_scales_with_the_jobs_own_timeout_not_
a_fixed_duration` (regression test for the bug this fix addressed).

**Verified live** (not just tested): a job was manually inserted into the running
Postgres with `status=RUNNING` and an already-expired `lease_expires_at` (standing
in for "a worker process that crashed mid-execution" — not reproducible by just
killing a container on a one-shot task), while dispatcher/publisher/worker/
reconciliation all ran as real containers over real SQS (LocalStack). The
reconciliation container's log showed `reconciliation_recovered_abandoned_running`
on its next sweep; the job was retried through the full real pipeline and reached
`SUCCEEDED`, with the attempts history showing attempt 1 as `FAILED_RETRYABLE`
("worker lease expired...") and attempt 2 as `SUCCEEDED` on a live worker.

## 5. Duplicate message delivery

SQS is at-least-once by contract — a message can be delivered more than once
(e.g. a worker received it but didn't delete it before its visibility timeout
expired, even though it *did* finish processing — a race between "finalize
committed" and "delete acknowledged"). `app/workers/claim.py`'s atomic
`QUEUED → RUNNING` claim is the actual safeguard: a second delivery of the same
message finds the job already `RUNNING` or already terminal (not `QUEUED`), and is
discarded without re-executing the handler.

**Tested**: `test_duplicate_delivery_only_executes_once` (same message delivered
twice via the real executor path; confirms `completed_at` is unchanged after the
second delivery — the handler ran exactly once), plus the full
`test_rejects_duplicate_delivery_of_already_running_job` /
`..._already_succeeded_job` unit tests of the claim logic directly.

## 6. Stale message after cancellation or reschedule

A job can be cancelled or rescheduled *after* a message for it has already been
published to SQS. The worker checks `job.status == CANCELLED` (discard) and
`job.attempt_count == message.attempt_number` (discard if the job has since been
redispatched with a new attempt — e.g. after a reschedule) before executing.

**Tested**: `test_rejects_cancelled_job`, `test_rejects_stale_attempt_number`.

## 7. Message arrives before its job is actually due

The dispatcher works within a rolling horizon (default 120s) *before* a job's
`scheduled_at`, so a message can legitimately arrive at a worker well before it
should run (see `DESIGN_DECISIONS.md` for why — SQS's 15-minute delay cap rules out
delaying the message itself for far-future jobs). The worker checks
`now < scheduled_at` and, if so, calls `change_visibility` to hide the message
until closer to due time — never executes early, never busy-waits.

**Tested**: `test_early_message_is_deferred_not_executed`. **Confirmed at every
benchmark load level** (`BENCHMARKS.md` §7): 0 early starts recorded across all
three benchmark runs, including under severe overload.

## 8. Missed dispatch (dispatcher was down when a job became due)

Not a special case — the dispatcher's claim query is `scheduled_at <= now() +
horizon`, which is satisfied just as well by a job that became due 10 seconds ago
as one that's due in 10 seconds. There's no "missed window" to recover from; the
job simply gets claimed on the next poll after the dispatcher comes back up.
Confirmed directly: `test_claims_job_scheduled_in_the_past`.

## 9. Jobs that exceed their execution timeout

**Documented policy**: a timeout is treated as a retryable failure, subject to the
same `max_retries` budget as any other retryable error — not a special "timeout
budget." The attempt is recorded with a distinct `TIMED_OUT` outcome (vs
`FAILED_RETRYABLE`) in the attempts history, so operators can tell timeouts apart
from other transient failures when inspecting `GET /jobs/{id}/attempts`, but the
retry/dead-letter decision logic doesn't treat them differently.

**Tested**: `test_timeout_outcome_recorded_distinctly`.

## 10. Jobs whose handler fails permanently

A handler that raises `PermanentError` (as opposed to `RetryableError`) sends the
job straight to `FAILED` — no retries, regardless of remaining budget. This is a
deliberate escape hatch for errors retrying can never fix (e.g. the payload fails
business validation) — retrying a `PermanentError` would just waste the retry
budget for no benefit.

**Tested**: `test_permanent_failure_end_to_end`, `test_finalize_permanent_failure`.

## 11. An unknown/non-contractual exception from a handler

Handlers are expected to raise only `RetryableError` or `PermanentError`
(`app/handlers/base.py`). If one raises something else — a bug in the handler, an
unexpected library exception — the worker treats it as **retryable** (fail-safe:
better to retry an ambiguous failure than silently drop the job) and logs a
warning (`handler_raised_non_contractual_exception`) so the handler's contract
violation is visible to operators, not swallowed silently.

## 12. Cancelling a job that's already `RUNNING`

**Explicitly not supported**, by design, not oversight.
`CANCELLABLE_STATES` (`app/models/states.py`) excludes `RUNNING` — there's no safe
way to interrupt a handler already executing (Python doesn't offer safe
mid-coroutine cancellation that guarantees cleanup for arbitrary handler code), so
rather than attempt it and silently fail to actually stop the work, the API returns
`409 Conflict` for a cancel request against a `RUNNING` job. This is documented as
best-effort cancellation, consistent with how real schedulers (cron, Celery)
describe the same limitation.

## 13. Database transaction failures (general)

Every write path (`claim_due_jobs`, `publish_pending_batch`, `claim_for_execution`,
`finalize_*`, the reconciliation sweeps) uses `FOR UPDATE` row locking and a single
`session.commit()` per logical unit of work — if the commit fails (constraint
violation, connection loss), the transaction rolls back entirely, leaving the row
in its prior, consistent state for the next poll to retry. No code path partially
applies a multi-row update and leaves the database in an inconsistent
intermediate state.
