"""Prometheus metrics shared across the API process.

Label cardinality rule: never label a metric with a job_id, idempotency_key, or any
other unbounded value. task_type and status are safe because the handler registry
and the state machine both bound their cardinality to a small fixed set.
"""
from prometheus_client import Counter, Histogram

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
