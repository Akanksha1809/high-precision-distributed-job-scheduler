import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config.settings import Settings
from app.observability.db_metrics import sample_pool_metrics
from app.observability.logging import get_logger
from app.observability.metrics import db_query_errors_total, queue_backlog
from app.publisher.outbox import publish_pending_batch
from app.queue.base import Queue

logger = get_logger(component="outbox_publisher")


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        queue: Queue,
        settings: Settings,
        engine: AsyncEngine,
    ):
        self._session_factory = session_factory
        self._queue = queue
        self._settings = settings
        self._engine = engine

    async def _publish_once(self) -> int:
        async with self._session_factory() as session:
            return await publish_pending_batch(
                session,
                self._queue,
                batch_size=self._settings.outbox_batch_size,
                max_publish_attempts=self._settings.outbox_max_publish_attempts,
            )

    async def _loop(self, loop_id: int, stop_event: asyncio.Event) -> None:
        poll_interval = self._settings.outbox_poll_interval_ms / 1000
        while not stop_event.is_set():
            if loop_id == 0:
                sample_pool_metrics(self._engine, service="outbox_publisher")
                try:
                    queue_backlog.labels(queue="main").set(await self._queue.approximate_size())
                except Exception:
                    pass  # best-effort observability only; never let this break the publish loop
            try:
                processed = await self._publish_once()
            except Exception:
                logger.error("outbox_publish_loop_error", loop_id=loop_id, exc_info=True)
                db_query_errors_total.labels(service="outbox_publisher").inc()
                await asyncio.sleep(poll_interval)
                continue
            if processed == 0:
                await asyncio.sleep(poll_interval)

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        logger.info(
            "outbox_publisher_starting", concurrency=self._settings.outbox_publisher_concurrency
        )
        tasks = [
            asyncio.create_task(self._loop(i, stop_event))
            for i in range(self._settings.outbox_publisher_concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            logger.info("outbox_publisher_stopped")
