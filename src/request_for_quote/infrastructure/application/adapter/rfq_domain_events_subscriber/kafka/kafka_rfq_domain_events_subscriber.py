import json
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber
from request_for_quote.application.rfq import event_handler
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.kafka_integration_event_publisher import IntegrationEventEnvelope


class KafkaRfqDomainEventsSubscriber(IRfqDomainEventsSubscriber):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
    ) -> None:
        self._consumer = consumer

        self._current_message: ConsumerRecord | None = None

    async def subscribe(self) -> AsyncIterator[RfqRegisteredIntegrationEvent]:
        try:
            async for message in self._consumer:
                self._current_message = message

                envelope = IntegrationEventEnvelope.model_validate_json(message.value.decode("utf-8"))

                event = self._deserialize(envelope)

                try:
                    yield event
                finally:
                    self._current_message = None

        finally:
            await self._consumer.stop()

    async def ack(self) -> None:  # これのせいで、逐次的な処理しか実はできない。
        message = self._current_message

        if message is None:
            raise RuntimeError(
                "No lifecycle message is being processed."
            )

        topic_partition = TopicPartition(
            message.topic,
            message.partition,
        )

        await self._consumer.commit(
            {
                topic_partition: message.offset + 1,
            }
        )

    def _deserialize(
        self,
        envelope: IntegrationEventEnvelope,
    ) -> RfqRegisteredIntegrationEvent:
        match (envelope.message_type, envelope.schema_version):
            case ("rfq.registered", 1):
                return RfqRegisteredIntegrationEvent.model_validate(
                    {
                        "event_id": envelope.message_id,
                        **envelope.payload,
                    }
                )

            case _:
                print(
                    f"[WARNING] "
                    f"Unsupported lifecycle event: {envelope.message_type}, {envelope.schema_version}."
                )
                raise ValueError(
                    f"Unknown lifecycle event: {envelope.message_type}, {envelope.schema_version}"
                )
            