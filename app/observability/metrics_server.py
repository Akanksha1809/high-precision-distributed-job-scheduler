"""Exposes /metrics for the background services (dispatcher, outbox-publisher,
worker, reconciliation) — the API already serves its own via a FastAPI route.
`start_http_server` runs a small blocking HTTP server on a background thread, so this
is safe to call once from an asyncio app without touching the event loop.
"""
from prometheus_client import start_http_server

from app.observability.logging import get_logger

logger = get_logger(component="metrics_server")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    logger.info("metrics_server_started", port=port)
