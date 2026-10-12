# Future Improvements

Ordered roughly by how directly the benchmark/testing work in this project points
at them — not a wishlist, a punch list derived from actual findings.

## Directly motivated by BENCHMARKS.md's bottleneck analysis

1. **Multiple API processes/replicas behind a load balancer.** The single highest-
   leverage change identified — `BENCHMARKS.md` §5 found the single `uvicorn`
   process as a contributing bottleneck at sustained load. Locally:
   `uvicorn --workers N`; on AWS: bump `api_desired_count` in the Terraform config
   (already supports N replicas with zero code changes, since the API is stateless
   behind the ALB).
2. **A connection pooler (PgBouncer locally, RDS Proxy on AWS) in front of
   Postgres.** The DB connection pool was observed hitting its ceiling (20/20) at
   peak during the 150 req/s benchmark. This matters more, not less, once #1 adds
   more API replicas each with their own pool.
3. **Re-run the benchmark suite against real AWS SQS** instead of LocalStack. The
   current numbers can't distinguish "Chronos's publisher code is slow" from
   "LocalStack's SQS emulation is slow" — these are likely different, and real SQS
   is the one that actually matters for the production throughput target.
4. **Scale dispatcher/outbox-publisher/worker horizontally and re-benchmark.**
   This project's benchmarks ran exactly one instance of each — the architecture
   was built for `SKIP LOCKED`-safe horizontal scaling from the start (verified by
   `test_concurrent_dispatchers_never_double_claim`), but that scaling was never
   benchmarked, only tested for correctness.

## Operational maturity

5. **Archival/cleanup job for old `jobs`/`attempts`/`outbox` rows.** The schema and
   indexes (particularly `jobs.created_at`) already support an archival sweep using
   the same cursor-pagination pattern `GET /jobs` uses, but no such job exists yet.
   Needed before this could run unattended for months.
6. **CI pipeline**: run the test suite and `terraform plan` (not `apply`) on every
   PR. Currently both are manual steps documented in `README.md` and
   `infra/aws/DEPLOYMENT.md` respectively.
7. **A real alerting destination.** `infra/aws/terraform/cloudwatch.tf`'s alarms
   default to no action (`alarm_notification_topic_arn = ""`) — wiring an actual
   SNS topic (→ email/Slack/PagerDuty) is a one-variable change once there's
   somewhere for alerts to go.
8. **Worker heartbeat renewal for very long-running handlers.** The current lease
   design (`job.timeout_seconds + job_lease_buffer_seconds`, set once at claim
   time) correctly avoids false-positive "abandoned" detection for any job within
   its declared timeout (see `DESIGN_DECISIONS.md` and the regression test for the
   bug this fixed), but a handler that needs to report "I'm still alive and making
   progress" mid-execution (vs just running within a pre-declared time budget)
   would need an actual periodic heartbeat write, which doesn't exist.

## Correctness/robustness extensions

9. **Idempotency key TTL enforcement.** `idempotency_key_ttl_seconds` exists as a
   setting but nothing currently expires old idempotency keys — they're unique
   forever, which is safe but means the `jobs.idempotency_key` unique index grows
   without bound. Low priority (jobs table archival from #5 would naturally handle
   this), but worth noting as not yet wired up.
10. **FIFO/ordering support for callers who actually need it.** The current design
    deliberately uses a standard SQS queue (see `DESIGN_DECISIONS.md`) because
    nothing in Chronos's own job model needs cross-job ordering. If a future use
    case needs strict ordering for a *group* of related jobs, that would need a
    FIFO queue (with its throughput ceiling) scoped to just that use case, not a
    wholesale switch — the `Queue` interface (`app/queue/base.py`) already
    abstracts this enough that a second, FIFO-backed queue instance could coexist
    with the main standard queue if ever needed.
11. **Payload schema validation per `task_type`.** Currently each handler validates
    its own payload shape at execution time (e.g. `demo.process_data` raising
    `PermanentError` if `items` isn't a list) — payload validation happens inside
    the handler, not at job-creation time. A per-task-type JSON Schema registry,
    checked in `POST /jobs`, would catch malformed payloads immediately instead of
    after a wasted dispatch/queue/claim cycle. Deliberately not built yet, to avoid
    adding a schema-registry abstraction before there was a second handler shape
    to prove it was worth generalizing for.
