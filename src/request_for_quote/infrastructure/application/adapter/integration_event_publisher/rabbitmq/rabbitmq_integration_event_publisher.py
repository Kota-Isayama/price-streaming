import dataclasses
import json

import aio_pika
from aio_pika.abc import AbstractExchange

from request_for_quote.application.integration.events.rfq import EventType, RfqRegisteredIntegrationEvent
from request_for_quote.application.port.integration_event_publisher import IIntegrationEventPublisher
from request_for_quote.application.port.outbox_repository import OutboxEvent


class RabbitmqIntegrationEventPublisher(IIntegrationEventPublisher):
    def __init__(self, exchange: AbstractExchange) -> None:
        self._exchange = exchange

    async def publish(
        self,
        event: OutboxEvent,
    ) -> None:
        message = aio_pika.Message(
            body=json.dumps(dataclasses.asdict(event)).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(event.event_id),
            type=event.event_type,
        )

        await self._exchange.publish(
            message=message,
            routing_key=self._routing_key(event),
            mandatory=True,
        )

    def _routing_key(
        self,
        event: OutboxEvent,
    ) -> str:
        match event.event_type:
            case EventType.RFQ_REGISTERED:
                return EventType.RFQ_REGISTERED.value
            case _:
                raise ValueError(
                    f"Unsupported integration event: "
                    f"{event.event_type}"
                )
            