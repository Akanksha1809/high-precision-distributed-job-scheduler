"""Exponential backoff with jitter for retry scheduling.

Full jitter (not "equal" or "decorrelated" jitter): delay = random(0, min(cap, base *
2^attempt)). This is the AWS-recommended strategy for avoiding retry storms — if N
workers fail at the same instant (e.g. a downstream dependency blips), their next
attempts spread across the whole window instead of re-colliding at a shared instant.
"""
import random


def compute_backoff_seconds(
    attempt_count: int,
    *,
    base_delay_seconds: float,
    max_delay_seconds: float,
    jitter_seconds: float,
) -> float:
    """attempt_count is the attempt that just failed (1-indexed). Returns the delay
    before the next attempt should become eligible for dispatch."""
    exponential = base_delay_seconds * (2 ** max(attempt_count - 1, 0))
    capped = min(exponential, max_delay_seconds)
    jitter = random.uniform(0, jitter_seconds)
    return capped + jitter
