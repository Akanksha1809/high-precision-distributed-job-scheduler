"""Safe sample handlers used for demos, benchmarks, and retry/DLQ testing. None of
these execute user-supplied code or shell commands — they only operate on the
job's JSON payload.
"""
import asyncio
import random
from typing import Any

from app.handlers.base import PermanentError, RetryableError


async def simulate_notification(payload: dict[str, Any]) -> dict[str, Any]:
    """Pretends to send a notification (email/SMS/push). No real network call."""
    recipient = payload.get("recipient")
    message = payload.get("message", "")
    if not recipient:
        raise PermanentError("notification payload missing 'recipient'")
    await asyncio.sleep(random.uniform(0.01, 0.05))
    return {"sent_to": recipient, "message_length": len(message)}


async def process_data(payload: dict[str, Any]) -> dict[str, Any]:
    """A lightweight, deterministic CPU-ish transform used to exercise the pipeline
    with varying payload sizes without doing anything real-world-impactful."""
    items = payload.get("items")
    if not isinstance(items, list):
        raise PermanentError("process_data payload requires an 'items' list")
    await asyncio.sleep(random.uniform(0.01, 0.1))
    total = sum(float(x) for x in items if isinstance(x, (int, float)))
    return {"count": len(items), "sum": total}


async def benchmark_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Sleeps for a caller-specified duration. Used only by the load-testing suite
    to produce a controlled, realistic distribution of execution durations instead
    of every benchmark job taking the same fixed time."""
    duration_ms = float(payload.get("duration_ms", 10))
    await asyncio.sleep(duration_ms / 1000)
    return {"slept_ms": duration_ms}


async def always_fail(payload: dict[str, Any]) -> dict[str, Any]:
    """Intentionally fails every attempt — used in tests/demos to exercise retry
    backoff and dead-letter routing end to end."""
    mode = payload.get("mode", "retryable")
    await asyncio.sleep(0.01)
    if mode == "permanent":
        raise PermanentError("always_fail handler: permanent failure requested")
    raise RetryableError("always_fail handler: retryable failure requested")
