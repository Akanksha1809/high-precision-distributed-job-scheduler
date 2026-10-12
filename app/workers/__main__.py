import asyncio
import signal

from app.config.settings import get_settings
from app.db.session import dispose_engine, get_sessionmaker
from app.observability.logging import configure_logging, get_logger
from app.queue.factory import build_queue
from app.workers.service import Worker

logger = get_logger(component="worker")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    main_queue = build_queue(settings, name="main")
    dlq_queue = build_queue(settings, name="dlq")
    worker = Worker(get_sessionmaker(), main_queue, dlq_queue, settings)
    try:
        await worker.run_forever(stop_event)
    finally:
        await dispose_engine()
        logger.info("worker_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
