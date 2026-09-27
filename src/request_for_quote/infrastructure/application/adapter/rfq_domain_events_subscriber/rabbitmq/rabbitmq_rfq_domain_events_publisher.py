from typing import AsyncIterator

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber


class RabbitmqRfqDomainEventsSubscriber(IRfqDomainEventsSubscriber):
    def __init__(self, queue: AbstractQueue) -> None:
        self._queue = queue
        self._current_message: AbstractIncomingMessage | None = None

    async def subscribe(self) -> AsyncIterator[RfqRegisteredIntegrationEvent]:
        async with self._queue.iterator() as messages:
            async for message in messages:
                self._current_message = message

                event = RfqRegisteredIntegrationEvent.model_validate_json(message.body)

                yield event

    async def ack(self) -> None:
        if self._current_message is None:
            raise RuntimeError("No message to acknowledge.")

        await self._current_message.ack()
        self._current_message = None
