import abc
from typing import AsyncIterator

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent


class IRfqDomainEventsSubscriber(abc.ABC):
    @abc.abstractmethod
    def subscribe(self) -> AsyncIterator[RfqRegisteredIntegrationEvent]:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self) -> None:  # これはKafka前提のインターフェースになってしまってないか？
        raise NotImplementedError