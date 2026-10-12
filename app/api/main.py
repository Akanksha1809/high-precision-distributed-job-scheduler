import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.api.routes import jobs, system
from app.config.settings import get_settings
from app.db.session import dispose_engine
from app.observability.logging import configure_logging, get_logger
from app.observability.metrics import http_request_duration_seconds, http_requests_total

logger = get_logger(component="api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging(get_settings())
    logger.info("api_starting")
    yield
    await dispose_engine()
    logger.info("api_stopped")


app = FastAPI(title="Chronos", version="0.1.0", lifespan=lifespan)
app.include_router(system.router)
app.include_router(jobs.router)


class MetricsMiddleware:
    """Plain ASGI middleware (not Starlette's BaseHTTPMiddleware) — BaseHTTPMiddleware
    runs the downstream app in a separate task via anyio's task group, which has a
    known incompatibility with asyncpg/SQLAlchemy async connections ("attached to a
    different loop"). A raw ASGI middleware runs in the same task as the request, so
    there's no connection handoff across loops/tasks.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)

        duration = time.perf_counter() - start
        route = scope.get("route")
        path_template = route.path if route is not None else scope.get("path", "unknown")
        http_requests_total.labels(
            method=scope["method"], path=path_template, status_code=str(status_code)
        ).inc()
        http_request_duration_seconds.labels(
            method=scope["method"], path=path_template
        ).observe(duration)


app.add_middleware(MetricsMiddleware)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("unhandled_exception", path=request.url.path, error=str(exc), exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})
