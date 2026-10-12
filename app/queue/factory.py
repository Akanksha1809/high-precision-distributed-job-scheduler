from functools import lru_cache

from app.config.settings import Settings
from app.queue.base import Queue
from app.queue.memory import InMemoryQueue


@lru_cache
def _memory_queue(name: str) -> InMemoryQueue:
    # Process-local singleton per name: every caller in QUEUE_BACKEND=memory mode
    # within the same process must share one queue instance per logical queue (main,
    # dlq), or the publisher/worker would each see their own empty queue. This is
    # exactly why "memory" is a dev/test-only backend — it does not work across
    # separate containers.
    return InMemoryQueue()


def build_queue(settings: Settings, *, name: str = "main") -> Queue:
    if settings.queue_backend == "memory":
        return _memory_queue(name)

    if settings.queue_backend == "sqs":
        from app.queue.sqs import SQSQueue

        queue_url = settings.sqs_queue_url if name == "main" else settings.sqs_dlq_url
        if not queue_url:
            raise ValueError(
                f"QUEUE_BACKEND=sqs but no queue URL configured for '{name}' "
                f"(set SQS_QUEUE_URL / SQS_DLQ_URL)"
            )
        return SQSQueue(
            queue_url=queue_url,
            region=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url,
            wait_time_seconds=settings.sqs_wait_time_seconds,
            default_visibility_timeout_seconds=settings.sqs_visibility_timeout_seconds,
        )

    raise ValueError(f"Unknown queue_backend: {settings.queue_backend}")
