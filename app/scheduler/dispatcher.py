import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config.settings import Settings
from app.observability.db_metrics import sample_pool_metrics
from app.observability.logging import get_logger
from app.observability.metrics import db_query_errors_total
from app.scheduler.claim import claim_due_jobs

logger = get_logger(component="dispatcher")


class Dispatcher:
    """Runs `dispatch_concurrency` concurrent claim loops. Concurrency here models
    what happens across horizontally-scaled dispatcher replicas: every loop (whether
    in this process or another container entirely) safely races on the same jobs
    table via FOR UPDATE SKIP LOCKED, so adding more loops/replicas never causes
    double-dispatch.
    """

    def __init__(self, session_factory: async_sessionmaker, settings: Settings, engine: AsyncEngine):
        self._session_factory = session_factory
        self._settings = settings
        self._engine = engine

    async def _claim_once(self) -> int:
        async with self._session_factory() as session:
            claimed = await claim_due_jobs(
                session,
                horizon_seconds=self._settings.dispatch_horizon_seconds,
                batch_size=self._settings.dispatch_batch_size,
                dispatcher_id=self._settings.dispatcher_id,
            )
            if claimed:
                logger.info("jobs_claimed", count=len(claimed))
            return len(claimed)

    async def _loop(self, loop_id: int, stop_event: asyncio.Event) -> None:
        poll_interval = self._settings.dispatch_poll_interval_ms / 1000
        while not stop_event.is_set():
            if loop_id == 0:
                sample_pool_metrics(self._engine, service="dispatcher")
            try:
                claimed_count = await self._claim_once()
            except Exception:
                logger.error("dispatch_loop_error", loop_id=loop_id, exc_info=True)
                db_query_errors_total.labels(service="dispatcher").inc()
                claimed_count = 0
                await asyncio.sleep(poll_interval)
                continue
            if claimed_count == 0:
                await asyncio.sleep(poll_interval)

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        logger.info(
            "dispatcher_starting",
            dispatcher_id=self._settings.dispatcher_id,
            concurrency=self._settings.dispatch_concurrency,
            horizon_seconds=self._settings.dispatch_horizon_seconds,
        )
        tasks = [
            asyncio.create_task(self._loop(i, stop_event))
            for i in range(self._settings.dispatch_concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            logger.info("dispatcher_stopped")
