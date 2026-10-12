"""Queue abstraction. Business logic (dispatcher, outbox publisher, worker) talks
only to this interface, never to boto3/SQS or any other provider directly. This is
what lets the same code run against an in-memory queue in unit tests, LocalStack in
docker-compose, and real AWS SQS in production, by swapping one adapter.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    body: dict[str, Any]
    # Used by the SQS adapter for message deduplication (FIFO queues) and is always
    # passed even for standard queues so switching queue types later needs no call-site
    # changes. Convention: f"{job_id}:{attempt_number}".
    dedupe_key: str


@dataclass(frozen=True, slots=True)
class SendResult:
    dedupe_key: str
    success: bool
    provider_message_id: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    receipt_handle: str
    body: dict[str, Any]
    approximate_receive_count: int = 1


class Queue(ABC):
    """One logical queue. A dead-letter queue is just another `Queue` instance."""

    @abstractmethod
    async def send_batch(self, messages: list[OutboundMessage]) -> list[SendResult]:
        """At-least-once delivery. May partially fail — check each SendResult."""

    @abstractmethod
    async def receive(
        self, max_messages: int, wait_time_seconds: int
    ) -> list[ReceivedMessage]:
        """Long-poll for up to max_messages. Returns [] on timeout, never raises for
        an empty queue."""

    @abstractmethod
    async def delete(self, receipt_handle: str) -> None:
        """Acknowledge successful processing — removes the message permanently."""

    @abstractmethod
    async def change_visibility(self, receipt_handle: str, visibility_timeout_seconds: int) -> None:
        """Extend or shorten how long this message stays invisible to other
        consumers after being received. Used to defer a message that arrived early
        without busy-waiting, and to extend the lease for a long-running task."""

    @abstractmethod
    async def approximate_size(self) -> int:
        """Best-effort backlog depth, for the queue_backlog/dlq_depth metrics. SQS
        documents this as approximate (eventually consistent), not exact — callers
        must not rely on it for correctness, only for observability."""
