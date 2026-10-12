from sqlalchemy.ext.asyncio import AsyncEngine

from app.observability.metrics import db_pool_checked_out, db_pool_size


def sample_pool_metrics(engine: AsyncEngine, *, service: str) -> None:
    """Cheap, synchronous read of the connection pool's current counters — call this
    once per loop iteration in any long-running service. Pool internals expose plain
    ints, no I/O involved.
    """
    pool = engine.pool
    db_pool_size.labels(service=service).set(pool.size())
    db_pool_checked_out.labels(service=service).set(pool.checkedout())
