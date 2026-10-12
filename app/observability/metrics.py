"""Prometheus metrics shared across the API process.

Label cardinality rule: never label a metric with a job_id, idempotency_key, or any
other unbounded value. task_type and status are safe because the handler registry
and the state machine both bound their cardinality to a small fixed set.
"""
from prometheus_client import Counter, Gauge, Histogram

http_requests_total = Counter(
    "chronos_http_requests_total",
    "HTTP requests handled by the API",
    labelnames=("method", "path", "status_code"),
)

http_request_duration_seconds = Histogram(
    "chronos_http_request_duration_seconds",
    "HTTP request latency",
    labelnames=("method", "path"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

jobs_created_total = Counter(
    "chronos_jobs_created_total",
    "Jobs accepted via POST /jobs",
    labelnames=("task_type",),
)

jobs_idempotent_hits_total = Counter(
    "chronos_jobs_idempotent_hits_total",
    "POST /jobs requests that returned an existing job due to idempotency_key reuse",
)

jobs_cancelled_total = Counter(
    "chronos_jobs_cancelled_total",
    "Jobs cancelled via the API",
)

jobs_rescheduled_total = Counter(
    "chronos_jobs_rescheduled_total",
    "Jobs rescheduled via the API",
)

# --- Dispatcher ---
dispatcher_claimed_total = Counter(
    "chronos_dispatcher_claimed_total",
    "Jobs claimed (SCHEDULED -> DISPATCH_PENDING) by a dispatcher loop",
)

dispatcher_claim_duration_seconds = Histogram(
    "chronos_dispatcher_claim_duration_seconds",
    "Time to run one claim-batch transaction",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)

dispatcher_dispatch_lag_seconds = Histogram(
    "chronos_dispatcher_dispatch_lag_seconds",
    "Time between a job's scheduled_at and the moment it was claimed for dispatch",
    buckets=(0, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

# --- Outbox publisher ---
outbox_published_total = Counter(
    "chronos_outbox_published_total",
    "Outbox rows successfully published to the queue",
)

outbox_publish_failed_total = Counter(
    "chronos_outbox_publish_failed_total",
    "Outbox row publish attempts that failed (will retry until outbox_max_publish_attempts)",
)

outbox_publish_exhausted_total = Counter(
    "chronos_outbox_publish_exhausted_total",
    "Outbox rows that exhausted outbox_max_publish_attempts and are now stuck FAILED",
)

# --- Worker ---
jobs_succeeded_total = Counter(
    "chronos_jobs_succeeded_total", "Job attempts that completed successfully", labelnames=("task_type",)
)
jobs_failed_permanent_total = Counter(
    "chronos_jobs_failed_permanent_total",
    "Job attempts that failed permanently (non-retryable)",
    labelnames=("task_type",),
)
jobs_retried_total = Counter(
    "chronos_jobs_retried_total",
    "Job attempts that failed but were scheduled for retry",
    labelnames=("task_type",),
)
jobs_dead_lettered_total = Counter(
    "chronos_jobs_dead_lettered_total",
    "Jobs that exhausted their retry budget and were routed to the DLQ",
    labelnames=("task_type",),
)
jobs_timed_out_total = Counter(
    "chronos_jobs_timed_out_total",
    "Job attempts that exceeded their timeout_seconds",
    labelnames=("task_type",),
)
stale_messages_discarded_total = Counter(
    "chronos_stale_messages_discarded_total",
    "Queue messages discarded without execution (cancelled job, stale attempt, already-terminal job)",
    labelnames=("reason",),
)
messages_deferred_total = Counter(
    "chronos_messages_deferred_total",
    "Messages that arrived before their job's scheduled_at and were deferred via change_visibility",
)
worker_active_tasks = Gauge(
    "chronos_worker_active_tasks", "Number of job executions currently in flight on this worker"
)
task_execution_duration_seconds = Histogram(
    "chronos_task_execution_duration_seconds",
    "Wall-clock time spent inside a handler (excludes DB bookkeeping)",
    labelnames=("task_type",),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

# --- Reconciliation ---
reconciliation_recovered_total = Counter(
    "chronos_reconciliation_recovered_total",
    "Jobs recovered by the reconciliation sweep",
    labelnames=("reason",),
)
outbox_backlog = Gauge(
    "chronos_outbox_backlog", "Number of outbox rows currently PENDING (awaiting publish)"
)
outbox_oldest_pending_age_seconds = Gauge(
    "chronos_outbox_oldest_pending_age_seconds",
    "Age of the oldest PENDING outbox row, in seconds (0 if none pending)",
)
queue_backlog = Gauge(
    "chronos_queue_backlog", "Approximate number of messages on a queue", labelnames=("queue",)
)

# --- Database ---
db_pool_size = Gauge(
    "chronos_db_pool_size", "Configured size of this process's DB connection pool", labelnames=("service",)
)
db_pool_checked_out = Gauge(
    "chronos_db_pool_checked_out",
    "Connections currently checked out of the pool (in use)",
    labelnames=("service",),
)
db_query_errors_total = Counter(
    "chronos_db_query_errors_total",
    "Exceptions raised out of a DB-touching unit of work (claim/publish/execute/sweep)",
    labelnames=("service",),
)
