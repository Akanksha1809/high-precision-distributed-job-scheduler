from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.api.schemas import JobCreateRequest
from app.config.settings import get_settings


def test_valid_request_normalizes_to_utc():
    req = JobCreateRequest(
        task_type="demo.notify",
        payload={"recipient": "a@b.com"},
        scheduled_at="2026-10-13T05:30:00+05:30",
    )
    assert req.scheduled_at == datetime(2026, 10, 13, 0, 0, 0, tzinfo=timezone.utc)


def test_unregistered_task_type_rejected():
    with pytest.raises(ValidationError):
        JobCreateRequest(
            task_type="not.a.real.handler",
            payload={},
            scheduled_at="2026-10-13T00:00:00Z",
        )


def test_naive_datetime_rejected():
    with pytest.raises(ValidationError):
        JobCreateRequest(
            task_type="demo.notify",
            payload={},
            scheduled_at="2026-10-13T00:00:00",
        )


def test_oversized_payload_rejected():
    settings = get_settings()
    huge_payload = {"blob": "x" * (settings.job_payload_max_bytes + 1)}
    with pytest.raises(ValidationError):
        JobCreateRequest(
            task_type="demo.notify",
            payload=huge_payload,
            scheduled_at="2026-10-13T00:00:00Z",
        )


def test_max_retries_bounds():
    with pytest.raises(ValidationError):
        JobCreateRequest(
            task_type="demo.notify",
            payload={},
            scheduled_at="2026-10-13T00:00:00Z",
            max_retries=-1,
        )
    with pytest.raises(ValidationError):
        JobCreateRequest(
            task_type="demo.notify",
            payload={},
            scheduled_at="2026-10-13T00:00:00Z",
            max_retries=21,
        )
