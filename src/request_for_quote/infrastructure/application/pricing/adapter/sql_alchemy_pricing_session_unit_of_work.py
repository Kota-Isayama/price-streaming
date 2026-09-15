from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork
from request_for_quote.infrastructure.application.pricing.adapter.sql_alchemy_pricing_session_store import SqlAlchemyPricingSessionStore


class SqlAlchemyPricingSessionUnitOfWork(IPricingSessionUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def __aenter__(self):
        self._session = self._session_factory()

        self.session_store = SqlAlchemyPricingSessionStore(
            self._session
        )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            await self._session.rollback()

        await self._session.close()

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    def get_pricing_session_store(self) -> SqlAlchemyPricingSessionStore:
        return self.session_store
    