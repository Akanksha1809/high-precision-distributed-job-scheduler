import enum
import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class AttemptOutcome(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    TIMED_OUT = "TIMED_OUT"


_ALL_OUTCOMES = tuple(o.value for o in AttemptOutcome)


class AttemptRecord(Base):
    """Immutable history of one execution attempt of a job. Append-only: a job with
    3 attempts has 3 rows here, letting GET /jobs/{id}/attempts show the full retry
    history (worker, timing, error) rather than just the latest state."""

    __tablename__ = "attempts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(100))

    outcome: Mapped[str] = mapped_column(
        String(32), nullable=False, default=AttemptOutcome.RUNNING.value
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint(f"outcome IN {_ALL_OUTCOMES}", name="ck_attempts_outcome_valid"),
        Index("ix_attempts_job_id_attempt_number", "job_id", "attempt_number"),
    )
