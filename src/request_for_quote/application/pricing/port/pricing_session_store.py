import abc

from request_for_quote.application.pricing.session import PricingSession

class IPricingSessionStore(abc.ABC):
    async def save(
        self,
        pricing_session: PricingSession,
    ) -> None:
        raise NotImplementedError

    async def get_by_id(
        self,
        request_id: str,
    ) -> PricingSession:
        raise NotImplementedError

    async def list_active(
        self,
    ) -> list[PricingSession]:
        raise NotImplementedError
    