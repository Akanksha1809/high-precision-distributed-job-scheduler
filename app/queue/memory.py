"""In-process fake queue: FIFO delivery, at-least-once semantics (duplicates happen
on visibility-timeout expiry, exactly like SQS), used for unit tests and
QUEUE_BACKEND=memory local dev. Not shared across processes — each worker process
would see its own queue, which is why SQS/LocalStack is required once you run more
than one dispatcher/worker container.
"""
import asyncio
import time
import uuid
from collections import deque

from app.queue.base import OutboundMessage, Queue, ReceivedMessage, SendResult


class _InFlight:
    __slots__ = ("body", "dedupe_key", "visible_at", "receive_count")

    def __init__(self, body: dict, dedupe_key: str, visible_at: float, receive_count: int):
        self.body = body
        self.dedupe_key = dedupe_key
        self.visible_at = visible_at
        self.receive_count = receive_count


class InMemoryQueue(Queue):
    def __init__(self, default_visibility_timeout_seconds: int = 30):
        self._available: deque[tuple[dict, str]] = deque()
        self._in_flight: dict[str, _InFlight] = {}
        self._default_visibility_timeout = default_visibility_timeout_seconds
        self._new_message_event = asyncio.Event()

    def _requeue_expired(self) -> None:
        now = time.monotonic()
        expired = [h for h, m in self._in_flight.items() if m.visible_at <= now]
        for handle in expired:
            msg = self._in_flight.pop(handle)
            self._available.append((msg.body, msg.dedupe_key))
        if expired:
            self._new_message_event.set()

    async def send_batch(self, messages: list[OutboundMessage]) -> list[SendResult]:
        results = []
        for msg in messages:
            self._available.append((msg.body, msg.dedupe_key))
            results.append(
                SendResult(
                    dedupe_key=msg.dedupe_key,
                    success=True,
                    provider_message_id=str(uuid.uuid4()),
                )
            )
        self._new_message_event.set()
        return results

    async def receive(self, max_messages: int, wait_time_seconds: int) -> list[ReceivedMessage]:
        deadline = time.monotonic() + wait_time_seconds
        while True:
            self._requeue_expired()
            if self._available:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            self._new_message_event.clear()
            try:
                await asyncio.wait_for(self._new_message_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return []

        received: list[ReceivedMessage] = []
        now = time.monotonic()
        for _ in range(min(max_messages, len(self._available))):
            body, dedupe_key = self._available.popleft()
            handle = str(uuid.uuid4())
            self._in_flight[handle] = _InFlight(
                body=body,
                dedupe_key=dedupe_key,
                visible_at=now + self._default_visibility_timeout,
                receive_count=1,
            )
            received.append(ReceivedMessage(receipt_handle=handle, body=body))
        return received

    async def delete(self, receipt_handle: str) -> None:
        self._in_flight.pop(receipt_handle, None)

    async def change_visibility(self, receipt_handle: str, visibility_timeout_seconds: int) -> None:
        msg = self._in_flight.get(receipt_handle)
        if msg is not None:
            msg.visible_at = time.monotonic() + visibility_timeout_seconds

    async def approximate_size(self) -> int:
        self._requeue_expired()
        return len(self._available) + len(self._in_flight)
