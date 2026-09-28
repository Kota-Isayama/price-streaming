import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustQueue,
)

from request_for_quote.application.integration.events.integration_event_envelope import (
    IntegrationEventEnvelope,
)
from request_for_quote.application.integration.events.rfq import (
    RfqIntegrationEvent,
    RfqRegisteredIntegrationEvent,
)
from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)
from request_for_quote.application.pricing.model.rfq_domain_event_delivery import (
    RfqDomainEventDelivery,
    RfqDomainEventDeliveryId,
)
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import (
    IPricingRfqDomainEventsSubscriber,
)


@dataclass(frozen=True)
class _PendingRabbitMqDelivery:
    shard_id: PricingShardId
    message: AbstractIncomingMessage


class RabbitMqQuorumRfqDomainEventsSubscriber(
    IPricingRfqDomainEventsSubscriber,
):
    """
    Kafka版と違い、RabbitMQでは既にQueue自体がShard。

        shard=2
            ↓
        rfq.pricing.2 Queue

    なのでKafka版のような

        dispatcher.run()
            ↓
        asyncio.Queue[partition]

    は不要。

    subscribe(shard_id) が
    RabbitMQ Queueを直接consumeできる。
    """
    def __init__(
        self,
        *,
        queues: dict[int, AbstractRobustQueue],
    ) -> None:
        self._queues = queues

        self._pendings: dict[RfqDomainEventDeliveryId, _PendingRabbitMqDelivery] = {}

    async def subscribe(self, shard_id: PricingShardId) -> AsyncIterator[RfqDomainEventDelivery]:
        queue = self._queues.get(shard_id.value)

        if queue is None:
            raise ValueError(
                "Unknown pricing shard: "
                f"{shard_id.value}",
            )

        print(
            "[RABBITMQ SHARD SUBSCRIBE] "
            f"shard={shard_id.value}"
        )

        # no_ack=Falseなのでmanual ACK
        async with queue.iterator(no_ack=False) as iterator:
            async for message in iterator:
                envelope = IntegrationEventEnvelope.model_validate_json(
                    message.body.decode("utf-8"),
                )

                event = self._deserialize(envelope)

                delivery_id = RfqDomainEventDeliveryId(value=uuid.uuid4())
                delivery = RfqDomainEventDelivery(
                    delivery_id=delivery_id,
                    shard_id=shard_id,
                    event=event,
                )

                self._pendings[delivery_id] = _PendingRabbitMqDelivery(
                    shard_id=shard_id,
                    message=message,
                )

                try:
                    yield delivery
                finally:
                    # 正常時にはすでにack()で消えている。
                    #
                    # consumer Task cancel / exceptionなら
                    # pendingsだけから削除する
                    #
                    # RabbitMQにはACKを送らないため、
                    # channel / consumer終了時に再配送対象になる
                    self._pendings.pop(
                        delivery_id,
                        None,
                    )

    async def ack(
        self,
        delivery: RfqDomainEventDelivery,
    ) -> None:
        pending = self._pendings.pop(
            delivery.delivery_id,
            None,
        )

        if pending is None:
            raise RuntimeError(
                "Unknown RabbitMQ delivery: "
                f"{delivery.delivery_id}"
            )

        await pending.message.ack()

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