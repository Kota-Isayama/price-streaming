import abc
from typing import Self

from request_for_quote.application.port.outbox_repository import OutboxRepository
from request_for_quote.domain.pricing.request_repository import IPricingRequestRepository
from request_for_quote.domain.request_for_quote.repository import IRfqRepository


class IRfqUnitOfWork(abc.ABC):
    @abc.abstractmethod
    async def __aenter__(self) -> Self:
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_rfq_repository(self) -> IRfqRepository:
        raise NotImplementedError

    @abc.abstractmethod
    def get_pricing_request_repository(self) -> IPricingRequestRepository:
        raise NotImplementedError

    @abc.abstractmethod
    def get_outbox_repository(self) -> OutboxRepository:
        raise NotImplementedError
    