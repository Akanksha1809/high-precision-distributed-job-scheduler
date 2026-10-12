import enum
import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class OutboxStatus(str, enum.Enum):
    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


_ALL_STATUSES = tuple(s.value for s in OutboxStatus)


class OutboxRecord(Base):
    """Transactional outbox row.

    Written in the SAME database transaction that moves a job SCHEDULED ->
    DISPATCH_PENDING. This is what makes the dual-write between Postgres and SQS safe:
    if the process crashes after the DB commit but before publishing to SQS, the row
    is still here with status=PENDING and a separate publisher (or reconciliation) will
    pick it up and publish it. Nothing is ever "committed to Postgres and lost to SQS".
    """

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )

    # attempt_number at the time this outbox row was created; combined with job_id this
    # forms a natural dedupe token so a message published twice (retries of the publish
    # step itself) is safely ignorable by a worker that already handled it.
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)

    message_body: Mapped[dict] = mapped_column(JSONB, nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=OutboxStatus.PENDING.value
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint(f"status IN {_ALL_STATUSES}", name="ck_outbox_status_valid"),
        Index("ix_outbox_status_created_at", "status", "created_at"),
        Index("ix_outbox_job_id", "job_id"),
    )
