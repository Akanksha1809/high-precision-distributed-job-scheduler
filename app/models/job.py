import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.models.states import JobStatus

_ALL_STATUSES = tuple(s.value for s in JobStatus)


class Job(Base):
    """The authoritative record of a scheduled job. Postgres is the source of truth;
    everything downstream (outbox, SQS, workers) only ever reflects what's here."""

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Supplied by the client to make POST /jobs safe to retry. Unique at the DB level
    # (Postgres allows multiple NULLs in a unique column, so this is a no-op for jobs
    # created without a key).
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)
    # sha256 of the canonical (task_type, payload, scheduled_at) request body. Lets us
    # detect a key reused with a *different* request body and reject it as a conflict
    # instead of silently returning the wrong job.
    idempotency_request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=JobStatus.SCHEDULED.value
    )

    # Canonical scheduling timestamp, always UTC. For RETRY_PENDING -> SCHEDULED
    # transitions this is bumped forward to the next backoff time.
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    last_error: Mapped[str | None] = mapped_column(Text)
    dead_letter_reason: Mapped[str | None] = mapped_column(Text)

    # Which dispatcher instance claimed this job for dispatch (debugging / audit only;
    # claiming correctness comes from the SKIP LOCKED transaction, not this column).
    claimed_by: Mapped[str | None] = mapped_column(String(100))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Worker lease: set when a worker starts RUNNING a job, checked by reconciliation
    # to detect abandoned jobs (worker crashed mid-execution) once the lease expires.
    leased_by: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Optimistic concurrency token for API-level cancel/reschedule races.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        CheckConstraint(f"status IN {_ALL_STATUSES}", name="ck_jobs_status_valid"),
        Index("ix_jobs_status_scheduled_at", "status", "scheduled_at"),
        Index("ix_jobs_status_lease_expires_at", "status", "lease_expires_at"),
        Index("ix_jobs_status_dispatched_at", "status", "dispatched_at"),
        Index("ix_jobs_created_at", "created_at"),
    )
