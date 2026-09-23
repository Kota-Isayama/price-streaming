import asyncio
from datetime import datetime

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from request_for_quote.application.port.integration_event_publisher import IIntegrationEventPublisher
from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.event_router import KafkaEventRouter


class IntegrationEventEnvelope(BaseModel):
    message_id: str
    message_type: str
    schema_version: int
    occurred_at: datetime
    payload: dict


class KafkaIntegrationEventPublisher(IIntegrationEventPublisher):
    def __init__(self, *, producer: AIOKafkaProducer, router: KafkaEventRouter) -> None:
        self._producer = producer
        self._router = router

    async def publish(self, event: OutboxEvent) -> None:
        topic = self._router.resolve_topic(event_type=event.event_type)

        envelope = IntegrationEventEnvelope(
            message_id=event.event_id,
            message_type=event.event_type,
            schema_version=event.schema_version,
            occurred_at=event.occurred_at,
            payload=event.payload,
        )

        await self._producer.send_and_wait(
            topic,
            key=event.aggregate_id.encode("utf-8"),
            value=envelope.model_dump_json().encode("utf-8"),
        )

