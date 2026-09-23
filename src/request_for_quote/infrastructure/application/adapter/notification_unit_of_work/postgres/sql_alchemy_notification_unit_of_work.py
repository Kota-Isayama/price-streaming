# infrastructure/notification/postgres/unit_of_work.py

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from request_for_quote.application.port.notification_unit_of_work import INotificationUnitOfWork
from request_for_quote.infrastructure.application.adapter.notification_repository.postgres.sql_alchemy_notification_repository import SqlAlchemyNotificationRepository


class SqlAlchemyNotificationUnitOfWork(
    INotificationUnitOfWork
):
    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_maker = session_maker
        self._session: AsyncSession | None = None
        self._notifications = None

    @property
    def notifications(
        self,
    ) -> SqlAlchemyNotificationRepository:
        if self._notifications is None:
            raise RuntimeError(
                "UnitOfWork has not been entered."
            )

        return self._notifications

    async def __aenter__(self):
        self._session = self._session_maker()

        self._notifications = SqlAlchemyNotificationRepository(self._session)

        return self

    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        if self._session is None:
            return

        try:
            if exc_type is not None:
                await self._session.rollback()
        finally:
            await self._session.close()

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError(
                "UnitOfWork has not been entered."
            )

        await self._session.commit()

    async def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError(
                "UnitOfWork has not been entered."
            )

        await self._session.rollback()