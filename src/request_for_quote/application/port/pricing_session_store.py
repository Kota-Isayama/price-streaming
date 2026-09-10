import abc

from request_for_quote.application.pricing.session import PricingSession


class IPricingSessionStore(abc.ABC):
    @abc.abstractmethod
    def get(self, request_id: str) -> PricingSession | None:
        raise NotImplementedError

    @abc.abstractmethod
    def save(self, session: PricingSession) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, request_id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def list_all(self) -> list[PricingSession]:
        raise NotImplementedError
    