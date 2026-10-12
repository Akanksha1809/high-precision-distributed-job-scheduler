from functools import lru_cache

from app.config.settings import Settings
from app.queue.base import Queue
from app.queue.memory import InMemoryQueue


@lru_cache
def _shared_memory_queue() -> InMemoryQueue:
    # Process-local singleton: every caller in QUEUE_BACKEND=memory mode within the
    # same process must share one queue instance, or the publisher and worker would
    # each see an empty queue of their own. This is exactly why "memory" is a
    # dev/test-only backend — it does not work across separate containers.
    return InMemoryQueue()


def build_queue(settings: Settings, *, name: str = "main") -> Queue:
    if settings.queue_backend == "memory":
        if name == "main":
            return _shared_memory_queue()
        return _named_memory_queue(name)
    if settings.queue_backend == "sqs":
        raise NotImplementedError("SQS backend lands in a later milestone")
    raise ValueError(f"Unknown queue_backend: {settings.queue_backend}")


@lru_cache
def _named_memory_queue(name: str) -> InMemoryQueue:
    return InMemoryQueue()
