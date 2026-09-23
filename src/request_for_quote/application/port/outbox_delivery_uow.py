# application/port/outbox_delivery_uow.py

import abc

from request_for_quote.application.port.outbox_repository import OutboxRepository


class OutboxDeliveryUnitOfWork(abc.ABC):

    # outbox: OutboxRepository 

    @abc.abstractmethod
    async def __aenter__(
        self,
    ) -> "OutboxDeliveryUnitOfWork":
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError