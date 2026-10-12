import asyncio

from app.queue.base import OutboundMessage
from app.queue.memory import InMemoryQueue


async def test_send_then_receive_then_delete():
    q = InMemoryQueue()
    await q.send_batch([OutboundMessage(body={"x": 1}, dedupe_key="k1")])
    received = await q.receive(max_messages=10, wait_time_seconds=1)
    assert len(received) == 1
    assert received[0].body == {"x": 1}
    await q.delete(received[0].receipt_handle)
    # Nothing left — a short wait should time out with no messages.
    received_again = await q.receive(max_messages=10, wait_time_seconds=0.2)
    assert received_again == []


async def test_receive_with_no_messages_times_out_without_blocking_forever():
    q = InMemoryQueue()
    received = await q.receive(max_messages=10, wait_time_seconds=0.2)
    assert received == []


async def test_undeleted_message_redelivered_after_visibility_timeout():
    q = InMemoryQueue(default_visibility_timeout_seconds=1)
    await q.send_batch([OutboundMessage(body={"x": 1}, dedupe_key="k1")])
    first = await q.receive(max_messages=10, wait_time_seconds=1)
    assert len(first) == 1
    # Not deleted -> invisible for ~1s, then redelivered (at-least-once semantics).
    immediately_again = await q.receive(max_messages=10, wait_time_seconds=0.1)
    assert immediately_again == []
    await asyncio.sleep(1.1)
    redelivered = await q.receive(max_messages=10, wait_time_seconds=0.5)
    assert len(redelivered) == 1
    assert redelivered[0].body == {"x": 1}


async def test_change_visibility_defers_redelivery():
    q = InMemoryQueue(default_visibility_timeout_seconds=1)
    await q.send_batch([OutboundMessage(body={"x": 1}, dedupe_key="k1")])
    msg = (await q.receive(max_messages=10, wait_time_seconds=1))[0]
    await q.change_visibility(msg.receipt_handle, visibility_timeout_seconds=3)
    # Original 1s timeout would have expired by now; the extension should prevent it.
    await asyncio.sleep(1.2)
    still_invisible = await q.receive(max_messages=10, wait_time_seconds=0.2)
    assert still_invisible == []


async def test_send_batch_preserves_fifo_order():
    q = InMemoryQueue()
    await q.send_batch(
        [
            OutboundMessage(body={"n": 1}, dedupe_key="a"),
            OutboundMessage(body={"n": 2}, dedupe_key="b"),
            OutboundMessage(body={"n": 3}, dedupe_key="c"),
        ]
    )
    received = await q.receive(max_messages=10, wait_time_seconds=1)
    assert [m.body["n"] for m in received] == [1, 2, 3]
