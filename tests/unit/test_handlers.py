import pytest

from app.handlers.base import PermanentError, RetryableError
from app.handlers.samples import always_fail, process_data, simulate_notification


async def test_simulate_notification_success():
    result = await simulate_notification({"recipient": "a@b.com", "message": "hi"})
    assert result["sent_to"] == "a@b.com"
    assert result["message_length"] == 2


async def test_simulate_notification_missing_recipient_is_permanent():
    with pytest.raises(PermanentError):
        await simulate_notification({})


async def test_process_data_success():
    result = await process_data({"items": [1, 2, 3.5]})
    assert result == {"count": 3, "sum": 6.5}


async def test_process_data_missing_items_is_permanent():
    with pytest.raises(PermanentError):
        await process_data({})


async def test_always_fail_default_is_retryable():
    with pytest.raises(RetryableError):
        await always_fail({})


async def test_always_fail_permanent_mode():
    with pytest.raises(PermanentError):
        await always_fail({"mode": "permanent"})
