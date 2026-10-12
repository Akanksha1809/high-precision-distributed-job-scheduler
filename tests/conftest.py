import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://chronos:chronos@postgres:5432/chronos_test"
)


@pytest_asyncio.fixture
async def db_engine():
    # Function-scoped (not session-scoped): an asyncpg connection pool is bound to the
    # event loop it was created on, and pytest-asyncio gives each test its own loop by
    # default. A session-scoped engine would hand out connections from a stale loop.
    engine = create_async_engine(TEST_DATABASE_URL, pool_pre_ping=True)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
def session_factory(db_engine):
    return async_sessionmaker(bind=db_engine, expire_on_commit=False, autoflush=False)


@pytest_asyncio.fixture
async def db_session(db_engine, session_factory) -> AsyncGenerator[AsyncSession, None]:
    """Fresh tables before every test. Repository functions commit internally, so a
    wrapping transaction can't give us rollback-based isolation — truncate instead."""
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE attempts, outbox, jobs CASCADE"))
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(session_factory, db_session) -> AsyncGenerator[AsyncClient, None]:
    """httpx client against the real FastAPI app (in-process, no network) with the
    DB session dependency overridden to use the test database."""
    from app.api.main import app
    from app.db.session import get_session

    async def _override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def anyio_backend():
    return "asyncio"
