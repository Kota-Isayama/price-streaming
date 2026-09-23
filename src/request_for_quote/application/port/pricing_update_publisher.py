import abc

from request_for_quote.domain.pricing.swap_pricer import SwapPrice


class IPricingUpdatePublisher(abc.ABC):
    async def publish(
        self,
        request_id: str,
        price: SwapPrice,
    ) -> None:
        raise NotImplementedError
    