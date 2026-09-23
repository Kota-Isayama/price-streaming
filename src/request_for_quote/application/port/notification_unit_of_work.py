import abc
from types import TracebackType
from typing import Self

from request_for_quote.application.port.notification_repository import INotificationRepository


class INotificationUnitOfWork(abc.ABC):
    @property
    @abc.abstractmethod
    def notifications(
        self,
    ) -> INotificationRepository:
        raise NotImplementedError

    @abc.abstractmethod
    async def __aenter__(self) -> Self:
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError