import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config.settings import Settings
from app.observability.logging import get_logger
from app.queue.base import Queue
from app.reconciliation.sweep import (
    recover_abandoned_running,
    recover_stale_dispatch_pending,
    update_outbox_backlog_gauges,
)

logger = get_logger(component="reconciliation")


class ReconciliationService:
    def __init__(self, session_factory: async_sessionmaker, dlq: Queue, settings: Settings):
        self._session_factory = session_factory
        self._dlq = dlq
        self._settings = settings

    async def _sweep_once(self) -> None:
        async with self._session_factory() as session:
            n = await recover_stale_dispatch_pending(
                session,
                stale_after_seconds=self._settings.reconciliation_stale_dispatch_seconds,
                batch_size=self._settings.reconciliation_batch_size,
            )
            if n:
                logger.info("recovered_stale_dispatch_pending", count=n)

        async with self._session_factory() as session:
            n = await recover_abandoned_running(
                session,
                batch_size=self._settings.reconciliation_batch_size,
                settings=self._settings,
                dlq=self._dlq,
            )
            if n:
                logger.info("recovered_abandoned_running", count=n)

        async with self._session_factory() as session:
            await update_outbox_backlog_gauges(session)

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        logger.info(
            "reconciliation_starting",
            interval_seconds=self._settings.reconciliation_interval_seconds,
            stale_dispatch_seconds=self._settings.reconciliation_stale_dispatch_seconds,
            lease_buffer_seconds=self._settings.job_lease_buffer_seconds,
        )
        while not stop_event.is_set():
            try:
                await self._sweep_once()
            except Exception:
                logger.error("reconciliation_sweep_error", exc_info=True)
            await asyncio.sleep(self._settings.reconciliation_interval_seconds)
        logger.info("reconciliation_stopped")
