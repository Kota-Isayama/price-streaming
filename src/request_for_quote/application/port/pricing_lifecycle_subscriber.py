import abc
from typing import AsyncIterator

from request_for_quote.application.pricing.lifecycle import RfqPricingLifecycleEvent


class IPricingLifecycleSubscriber(abc.ABC):
    @abc.abstractmethod
    def subscribe(self) -> AsyncIterator[RfqPricingLifecycleEvent]:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self) -> None:  # これはKafka前提のインターフェースになってしまってないか？
        raise NotImplementedError