import asyncio
from dataclasses import dataclass
from typing import AsyncIterator
import uuid

from rstream import AMQPMessage, Consumer, MessageContext

from request_for_quote.application.integration.events.integration_event_envelope import IntegrationEventEnvelope
from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.model.rfq_domain_event_delivery import RfqDomainEventDelivery, RfqDomainEventDeliveryId
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber


@dataclass(frozen=True)
class _PendingSuperStreamDelivery:
    consumer: Consumer
    stream: str
    offset: int
    subscriber_name: str


class RabbitMqSuperStreamPricingRfqDomainEventsSubscriber(
    IPricingRfqDomainEventsSubscriber
):
    def __init__(
        self,
        super_stream: str,
    ) -> None:
        self.super_stream = super_stream

        self._queues: dict[
            PricingShardId,
            asyncio.Queue[RfqDomainEventDelivery]
        ] = {}

        self._pendings: dict[
            RfqDomainEventDeliveryId,
            _PendingSuperStreamDelivery,
        ] = {}

    async def on_message(
        self,
        message: AMQPMessage,
        context: MessageContext,
    ) -> None:
        shard_id = self._shard_id_from_stream(
            context.stream,
        )

        envelope = IntegrationEventEnvelope.model_validate_json(
            message.body.decode("utf-8"),
        )

        event = self._deserialize(envelope)

        delivery_id = RfqDomainEventDeliveryId(
            value=str(uuid.uuid4()),
        )

        delivery = RfqDomainEventDelivery(
            delivery_id=delivery_id,
            shard_id=shard_id,
            event=event,
        )

        if context.subscriber_name is None:
            raise RuntimeError(
                "Super Stream consumer has no subscriber name"
            )

        self._pendings[delivery_id] = _PendingSuperStreamDelivery(
            consumer=context.consumer,
            stream=context.stream,
            offset=context.offset,
            subscriber_name=context.subscriber_name,
        )

        queue = self._queues.setdefault(
            shard_id,
            asyncio.Queue(),
        )

        await queue.put(delivery)

    async def subscribe(self, shard_id: PricingShardId) -> AsyncIterator[RfqDomainEventDelivery]:
        queue = self._queues.setdefault(
            shard_id,
            asyncio.Queue(),
        )

        while True:
            yield await queue.get()

    async def ack(self, delivery: RfqDomainEventDelivery) -> None:
        pending = self._pendings.pop(
            delivery.delivery_id,
            None,
        )

        if pending is None:
            raise RuntimeError(
                "Unknown Super Stream delivery: "
                f"{delivery.delivery_id}"
            )

        await pending.consumer.store_offset(
            stream=pending.stream,
            subscriber_name=pending.subscriber_name,
            offset=pending.offset,
        )

    def _shard_id_from_stream(
        self,
        stream: str,
    ) -> PricingShardId:
        prefix = f"{self._super_stream}-"

        if not stream.startswith(prefix):
            raise ValueError(
                f"Unexpected stream: {stream}"
            )

        return PricingShardId(
            int(stream.removeprefix(prefix))
        )

    def _deserialize(
            self,
            envelope: IntegrationEventEnvelope,
        ) -> RfqIntegrationEvent:
            match (
                envelope.message_type,
                envelope.schema_version,
            ):
                case ("rfq.registered", 1):
                    return (
                        RfqRegisteredIntegrationEvent
                        .model_validate(
                            {
                                "event_id":
                                    envelope.message_id,
                                **envelope.payload,
                            }
                        )
                    )
    
                case _:
                    raise ValueError(
                        "Unsupported RFQ domain event: "
                        f"type="
                        f"{envelope.message_type}, "
                        f"schema_version="
                        f"{envelope.schema_version}"
                    )
                