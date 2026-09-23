from request_for_quote.application.port.outbox_delivery_uow import OutboxDeliveryUnitOfWork
from request_for_quote.infrastructure.application.adapter.outbox_repository.sql_alchemy_repository import SqlAlchemyOutboxRepository


class SqlAlchemyOutboxDeliveryUnitOfWork(
    OutboxDeliveryUnitOfWork
):
    def __init__(
        self,
        session_maker,
    ) -> None:
        self._session_maker = session_maker

    async def __aenter__(self):
        self._session = self._session_maker()

        self.outbox = SqlAlchemyOutboxRepository(
            self._session
        )

        return self

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ):
        if exc is not None:
            await self.rollback()

        await self._session.close()
        