from dataclasses import dataclass, asdict
import datetime
import json

import aio_pika
from aio_pika.abc import AbstractExchange

from request_for_quote.application.integration.events.integration_event_envelope import IntegrationEventEnvelope
from request_for_quote.application.port.integration_event_publisher import IIntegrationEventPublisher
from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.application.pricing.port.pricing_shard_resolver import (
    IPricingShardResolver,
)


class RabbitMqIntegrationEventPublisher(IIntegrationEventPublisher):
    def __init__(
        self,
        *,
        exchange: AbstractExchange,
        shard_resolver: IPricingShardResolver,
    ) -> None:
        self._exchange = exchange
        self._shard_resolver = (
            shard_resolver
        )

    async def publish(
        self,
        event: OutboxEvent,
    ) -> None:
        shard_id = (
            self._shard_resolver.resolve(
                event.aggregate_id,
            )
        )

        envelope = IntegrationEventEnvelope(
            message_id=event.event_id,
            message_type=event.event_type,
            schema_version=event.schema_version,
            occurred_at=event.occurred_at,
            payload=event.payload,
        )

        await self._exchange.publish(
            aio_pika.Message(
                body=envelope.model_dump_json().encode("utf-8"),
                delivery_mode=(
                    aio_pika.DeliveryMode.PERSISTENT  # 何？
                ),
            ),
            routing_key=(
                f"pricing.{shard_id.value}"
            ),
            mandatory=True,
        )
