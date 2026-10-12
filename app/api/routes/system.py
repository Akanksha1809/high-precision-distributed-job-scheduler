from fastapi import APIRouter, Depends, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.handlers.registry import HANDLER_REGISTRY

router = APIRouter(tags=["system"])


@router.get("/health")
async def health() -> dict:
    """Liveness: the process is up and able to answer requests. Does not touch the DB
    — a slow/unreachable database should fail /ready, not make the process look dead."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(session: AsyncSession = Depends(get_session)) -> Response:
    """Readiness: can we actually serve traffic? Checks the database connection."""
    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        return Response(
            content='{"status":"not_ready","reason":"database_unreachable"}',
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            media_type="application/json",
        )
    return Response(content='{"status":"ready"}', media_type="application/json")


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/handlers")
async def list_handlers() -> dict:
    return {"registered_task_types": sorted(HANDLER_REGISTRY.keys())}
