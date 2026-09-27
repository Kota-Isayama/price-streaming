from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from request_for_quote.bootstrap.settings.database import (
    DatabaseSettings,
)


@asynccontextmanager
async def provide_session_maker(
    settings: DatabaseSettings,
) -> AsyncGenerator[
    async_sessionmaker[AsyncSession]
]:
    engine = create_async_engine(
        settings.url,
        echo=settings.echo,
    )

    session_maker = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )

    try:
        yield session_maker
    finally:
        await engine.dispose()
