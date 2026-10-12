import asyncio
import signal

from app.config.settings import get_settings
from app.db.session import dispose_engine, get_sessionmaker
from app.observability.logging import configure_logging, get_logger
from app.publisher.service import OutboxPublisher
from app.queue.factory import build_queue

logger = get_logger(component="outbox_publisher")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    queue = build_queue(settings, name="main")
    publisher = OutboxPublisher(get_sessionmaker(), queue, settings)
    try:
        await publisher.run_forever(stop_event)
    finally:
        await dispose_engine()
        logger.info("outbox_publisher_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
