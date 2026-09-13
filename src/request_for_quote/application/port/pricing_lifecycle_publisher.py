import abc

from request_for_quote.application.pricing.lifecycle import RfqPricingLifecycleEvent


class IPricingLifecyclePublisher(abc.ABC):
    @abc.abstractmethod
    async def publish(self, event: RfqPricingLifecycleEvent) -> None:
        raise NotImplementedError
    