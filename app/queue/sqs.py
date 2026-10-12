"""SQS adapter — the same Queue interface backing production (real AWS) and local
dev (LocalStack), selected purely by AWS_ENDPOINT_URL. Credentials always come from
boto3's standard credential provider chain (env vars, shared config, instance/task
role) — never hardcoded here. For LocalStack, dummy credentials are supplied via
environment variables in docker-compose, which is what LocalStack itself expects.

boto3 is synchronous; each call is offloaded to a thread via asyncio.to_thread so it
doesn't block the event loop. (We use boto3 rather than an async SQS library because
it's the actively-maintained, canonical AWS SDK for Python — aioboto3 wraps it anyway
and adds a second dependency surface for marginal benefit at our concurrency level.)
"""
import asyncio
import json

import boto3
from botocore.exceptions import ClientError

from app.queue.base import OutboundMessage, Queue, ReceivedMessage, SendResult

_SQS_SEND_BATCH_LIMIT = 10
_SQS_RECEIVE_LIMIT = 10


class SQSQueue(Queue):
    def __init__(
        self,
        *,
        queue_url: str,
        region: str,
        endpoint_url: str | None,
        wait_time_seconds: int,
        default_visibility_timeout_seconds: int,
    ):
        self._queue_url = queue_url
        self._wait_time_seconds = wait_time_seconds
        self._default_visibility_timeout = default_visibility_timeout_seconds
        self._client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)

    async def send_batch(self, messages: list[OutboundMessage]) -> list[SendResult]:
        results: list[SendResult] = []
        for i in range(0, len(messages), _SQS_SEND_BATCH_LIMIT):
            chunk = messages[i : i + _SQS_SEND_BATCH_LIMIT]
            results.extend(await self._send_chunk(chunk))
        return results

    async def _send_chunk(self, chunk: list[OutboundMessage]) -> list[SendResult]:
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps(msg.body)}
            for idx, msg in enumerate(chunk)
        ]
        idx_to_dedupe_key = {str(idx): msg.dedupe_key for idx, msg in enumerate(chunk)}

        try:
            response = await asyncio.to_thread(
                self._client.send_message_batch, QueueUrl=self._queue_url, Entries=entries
            )
        except ClientError as exc:
            # The whole batch call failed (e.g. network/auth error) — every message
            # in this chunk is unsent.
            return [
                SendResult(dedupe_key=m.dedupe_key, success=False, error=str(exc)) for m in chunk
            ]

        results: dict[str, SendResult] = {}
        for ok in response.get("Successful", []):
            key = idx_to_dedupe_key[ok["Id"]]
            results[key] = SendResult(
                dedupe_key=key, success=True, provider_message_id=ok.get("MessageId")
            )
        for fail in response.get("Failed", []):
            key = idx_to_dedupe_key[fail["Id"]]
            results[key] = SendResult(
                dedupe_key=key, success=False, error=fail.get("Message", "unknown SQS error")
            )
        return [results[m.dedupe_key] for m in chunk]

    async def receive(self, max_messages: int, wait_time_seconds: int) -> list[ReceivedMessage]:
        response = await asyncio.to_thread(
            self._client.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=min(max_messages, _SQS_RECEIVE_LIMIT),
            WaitTimeSeconds=min(wait_time_seconds, 20),  # SQS long-poll cap
            AttributeNames=["ApproximateReceiveCount"],
            VisibilityTimeout=self._default_visibility_timeout,
        )
        out = []
        for msg in response.get("Messages", []):
            attrs = msg.get("Attributes", {})
            out.append(
                ReceivedMessage(
                    receipt_handle=msg["ReceiptHandle"],
                    body=json.loads(msg["Body"]),
                    approximate_receive_count=int(attrs.get("ApproximateReceiveCount", 1)),
                )
            )
        return out

    async def delete(self, receipt_handle: str) -> None:
        await asyncio.to_thread(
            self._client.delete_message, QueueUrl=self._queue_url, ReceiptHandle=receipt_handle
        )

    async def change_visibility(self, receipt_handle: str, visibility_timeout_seconds: int) -> None:
        await asyncio.to_thread(
            self._client.change_message_visibility,
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout_seconds,
        )

    async def approximate_size(self) -> int:
        response = await asyncio.to_thread(
            self._client.get_queue_attributes,
            QueueUrl=self._queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
        attrs = response.get("Attributes", {})
        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        return visible + in_flight
