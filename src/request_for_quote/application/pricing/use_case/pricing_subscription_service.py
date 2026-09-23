from typing import AsyncIterator

from request_for_quote.application.port.pricing_update_subscriber import IPricingUpdateSubscriber, PricingUpdate


class PricingSubscriptionService:
    def __init__(
        self,
        subscriber: IPricingUpdateSubscriber,
    ) -> None:
        self._subscriber = subscriber

    async def set_subscription(
        self,
        request_ids: set[str],
    ) -> None:
        await self._subscriber.set_request_ids(request_ids)

    async def watch_price_updates(
        self,
    ) -> AsyncIterator[PricingUpdate]:
        async for update in self._subscriber.updates():
            yield update
        
    async def close(self) -> None:
        await self._subscriber.close()
