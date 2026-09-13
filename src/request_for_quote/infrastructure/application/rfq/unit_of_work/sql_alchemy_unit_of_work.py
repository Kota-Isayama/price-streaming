from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.infrastructure.application.adapter.outbox_repository.sql_alchemy_repository import SqlAlchemyOutboxRepository
from request_for_quote.infrastructure.domain.pricing_request.repository.sql_alchemy_repository import SqlAlchemyPricingRequestRepository
from request_for_quote.infrastructure.domain.request_for_quote.repository.sql_alchmey_repository import SqlAlchemyRfqRepository


class SqlAlchemyUnitOfWork(IRfqUnitOfWork):
    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_maker = session_maker

    async def __aenter__(self):
        self._session = self._session_maker()

        await self._session.begin()

        self.rfqs = SqlAlchemyRfqRepository(
            self._session
        )

        self.pricing_requests = (
            SqlAlchemyPricingRequestRepository(
                self._session
            )
        )

        self.outbox = SqlAlchemyOutboxRepository(
            self._session
        )

        return self

    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        try:
            if exc_type is not None:
                await self._session.rollback()
        finally:
            await self._session.close()

    async def commit(self) -> None:
        await self._session.commit()

    def get_rfq_repository(self):
        return self.rfqs

    def get_pricing_request_repository(self):
        return self.pricing_requests

    def get_outbox_repository(self):
        return self.outbox
    