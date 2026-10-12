"""Centralized, environment-driven configuration for every Chronos service."""
from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    # --- Database ---
    database_url: str = "postgresql+asyncpg://chronos:chronos@localhost:5432/chronos"
    db_pool_size: int = 10
    db_max_overflow: int = 10
    db_pool_timeout_seconds: int = 30
    db_statement_timeout_ms: int = 5000

    # --- Queue (abstracted: "memory" for tests/dev, "sqs" for LocalStack/AWS) ---
    queue_backend: Literal["memory", "sqs"] = "memory"
    aws_region: str = "us-east-1"
    aws_endpoint_url: str | None = None  # set to LocalStack URL in local compose
    sqs_queue_url: str | None = None
    sqs_dlq_url: str | None = None
    sqs_visibility_timeout_seconds: int = 60
    sqs_wait_time_seconds: int = 10  # long-poll
    sqs_max_messages_per_poll: int = 10

    # --- Dispatcher ---
    dispatch_horizon_seconds: int = 120
    dispatch_batch_size: int = 500
    dispatch_poll_interval_ms: int = 200
    dispatch_concurrency: int = 4
    dispatcher_id: str = "dispatcher-1"

    # --- Outbox publisher ---
    outbox_batch_size: int = 10  # SQS send_message_batch caps at 10 messages/call
    outbox_poll_interval_ms: int = 100
    outbox_publisher_concurrency: int = 4
    outbox_max_publish_attempts: int = 10

    # --- Worker ---
    worker_id: str = "worker-1"
    worker_concurrency: int = 8
    worker_heartbeat_interval_seconds: int = 10
    # Lease = job.timeout_seconds + this buffer (not a fixed duration) — see
    # app/workers/claim.py for why it must scale with each job's own timeout.
    job_lease_buffer_seconds: int = 30
    job_default_timeout_seconds: int = 30

    # --- Retry policy ---
    job_max_retries: int = 5
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 300.0
    retry_jitter_seconds: float = 1.0

    # --- Reconciliation ---
    reconciliation_interval_seconds: int = 30
    reconciliation_stale_dispatch_seconds: int = 60
    reconciliation_batch_size: int = 100

    # --- Payload limits ---
    job_payload_max_bytes: int = 256 * 1024  # 256 KiB

    # --- Observability ---
    metrics_port: int = 9100

    # --- API ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    idempotency_key_ttl_seconds: int = 86400


@lru_cache
def get_settings() -> Settings:
    return Settings()
