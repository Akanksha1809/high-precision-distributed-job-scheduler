"""Handler contract. A handler is a plain async function registered by name — jobs
reference handlers by `task_type` string, never by arbitrary code. This is what keeps
POST /jobs from being a remote-code-execution endpoint: the payload is just data fed
to a fixed, reviewed function.
"""
from collections.abc import Awaitable, Callable
from typing import Any

HandlerFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class RetryableError(Exception):
    """Raised by a handler for a transient failure — the job should be retried with
    backoff. Examples: a downstream dependency timed out, a lock was contended."""


class PermanentError(Exception):
    """Raised by a handler when retrying cannot help — the job goes straight to FAILED
    (or DEAD_LETTER once retries are exhausted by policy). Examples: payload fails
    business validation, a referenced resource doesn't exist."""
