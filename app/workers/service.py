import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config.settings import Settings
from app.observability.db_metrics import sample_pool_metrics
from app.observability.logging import get_logger
from app.observability.metrics import db_query_errors_total, queue_backlog
from app.queue.base import Queue
from app.workers.executor import process_message

logger = get_logger(component="worker")


class Worker:
    """Receives messages and executes them with bounded concurrency. Graceful
    shutdown: stop polling for new messages immediately, but let every in-flight
    `process_message` call finish naturally (it always runs to a durable DB commit
    and a queue ack before returning) rather than killing it mid-execution.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        main_queue: Queue,
        dlq_queue: Queue,
        settings: Settings,
        engine: AsyncEngine,
    ):
        self._session_factory = session_factory
        self._main_queue = main_queue
        self._dlq_queue = dlq_queue
        self._settings = settings
        self._engine = engine
        self._semaphore = asyncio.Semaphore(settings.worker_concurrency)
        self._in_flight: set[asyncio.Task] = set()

    async def _handle(self, msg) -> None:
        async with self._semaphore:
            try:
                await process_message(
                    msg,
                    session_factory=self._session_factory,
                    main_queue=self._main_queue,
                    dlq_queue=self._dlq_queue,
                    settings=self._settings,
                    worker_id=self._settings.worker_id,
                )
            except Exception:
                logger.error("message_processing_error", exc_info=True)
                db_query_errors_total.labels(service="worker").inc()

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        logger.info(
            "worker_starting",
            worker_id=self._settings.worker_id,
            concurrency=self._settings.worker_concurrency,
        )
        poll_count = 0
        try:
            while not stop_event.is_set():
                # Only pull as many new messages as we have free concurrency slots,
                # so a slow burst of long-running jobs doesn't pile up more
                # in-flight messages than we can actually execute at once.
                free_slots = self._settings.worker_concurrency - len(self._in_flight)
                if free_slots <= 0:
                    await asyncio.sleep(0.05)
                    self._in_flight = {t for t in self._in_flight if not t.done()}
                    continue

                poll_count += 1
                if poll_count % 20 == 0:  # don't sample on every poll — cheap but unnecessary
                    sample_pool_metrics(self._engine, service="worker")
                    try:
                        queue_backlog.labels(queue="main").set(
                            await self._main_queue.approximate_size()
                        )
                    except Exception:
                        pass  # best-effort observability only

                messages = await self._main_queue.receive(
                    max_messages=min(free_slots, self._settings.sqs_max_messages_per_poll),
                    wait_time_seconds=self._settings.sqs_wait_time_seconds,
                )
                for msg in messages:
                    task = asyncio.create_task(self._handle(msg))
                    self._in_flight.add(task)
                self._in_flight = {t for t in self._in_flight if not t.done()}
        finally:
            logger.info("worker_draining", in_flight=len(self._in_flight))
            if self._in_flight:
                await asyncio.gather(*self._in_flight, return_exceptions=True)
            logger.info("worker_stopped")
