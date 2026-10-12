import asyncio
import signal

from app.config.settings import get_settings
from app.db.session import dispose_engine, get_sessionmaker
from app.observability.logging import configure_logging, get_logger
from app.queue.factory import build_queue
from app.reconciliation.service import ReconciliationService

logger = get_logger(component="reconciliation")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    dlq = build_queue(settings, name="dlq")
    service = ReconciliationService(get_sessionmaker(), dlq, settings)
    try:
        await service.run_forever(stop_event)
    finally:
        await dispose_engine()
        logger.info("reconciliation_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
