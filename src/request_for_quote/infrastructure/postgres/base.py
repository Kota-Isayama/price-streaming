from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

AsyncSessionMaker = async_sessionmaker[AsyncSession]


class Base(DeclarativeBase):
    pass


def create_engine(
    database_url: str,
) -> AsyncEngine:
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        # echo=True,
    )


def create_session_maker(
    engine: AsyncEngine,
) -> AsyncSessionMaker:
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
