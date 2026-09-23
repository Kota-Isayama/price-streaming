import abc

from request_for_quote.application.port.outbox_repository import OutboxEvent


class IIntegrationEventPublisher(abc.ABC):
    @abc.abstractmethod
    async def publish(self, event: OutboxEvent) -> None:
        raise NotImplementedError
    