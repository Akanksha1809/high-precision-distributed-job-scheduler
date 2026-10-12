import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from app.config.settings import get_settings
from app.handlers.registry import is_registered


class JobCreateRequest(BaseModel):
    task_type: str = Field(..., examples=["demo.notify"])
    payload: dict[str, Any] = Field(default_factory=dict)
    scheduled_at: datetime = Field(..., examples=["2026-10-12T18:00:00Z"])
    idempotency_key: str | None = Field(default=None, max_length=255)
    max_retries: int | None = Field(default=None, ge=0, le=20)
    timeout_seconds: int | None = Field(default=None, ge=1, le=3600)

    @field_validator("task_type")
    @classmethod
    def task_type_must_be_registered(cls, v: str) -> str:
        if not is_registered(v):
            raise ValueError(f"Unknown task_type '{v}'. See GET /handlers for valid values.")
        return v

    @field_validator("scheduled_at")
    @classmethod
    def scheduled_at_must_be_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("scheduled_at must be timezone-aware (e.g. end with 'Z' or '+00:00')")
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def payload_size_limit(self) -> "JobCreateRequest":
        settings = get_settings()
        size = len(json.dumps(self.payload).encode("utf-8"))
        if size > settings.job_payload_max_bytes:
            raise ValueError(
                f"payload too large: {size} bytes exceeds limit of "
                f"{settings.job_payload_max_bytes} bytes"
            )
        return self


class JobRescheduleRequest(BaseModel):
    scheduled_at: datetime

    @field_validator("scheduled_at")
    @classmethod
    def scheduled_at_must_be_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("scheduled_at must be timezone-aware (e.g. end with 'Z' or '+00:00')")
        return v.astimezone(timezone.utc)


class JobResponse(BaseModel):
    id: uuid.UUID
    task_type: str
    payload: dict[str, Any]
    status: str
    scheduled_at: datetime
    max_retries: int
    attempt_count: int
    timeout_seconds: int
    created_at: datetime
    updated_at: datetime
    dispatched_at: datetime | None
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None
    dead_letter_reason: str | None
    idempotency_key: str | None

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    items: list[JobResponse]
    next_cursor: str | None
    limit: int


class AttemptResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    attempt_number: int
    worker_id: str | None
    outcome: str
    started_at: datetime
    completed_at: datetime | None
    duration_ms: int | None
    error_message: str | None

    model_config = {"from_attributes": True}


class ErrorResponse(BaseModel):
    detail: str
