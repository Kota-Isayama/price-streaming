import abc

from request_for_quote.application.pricing.port.pricing_session_store import IPricingSessionStore


class IPricingSessionUnitOfWork(abc.ABC):
    @abc.abstractmethod
    async def __aenter__(self) -> "IPricingSessionUnitOfWork":
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_pricing_session_store(self) -> IPricingSessionStore:
        raise NotImplementedError
