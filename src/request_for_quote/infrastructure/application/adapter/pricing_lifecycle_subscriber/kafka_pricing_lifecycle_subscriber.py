import json
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition

from request_for_quote.application.port.pricing_lifecycle_subscriber import IPricingLifecycleSubscriber
from request_for_quote.application.pricing.lifecycle import PricingActivated, PricingChanged, PricingStopped, RfqPricingLifecycleEvent


class KafkaPricingLifecycleSubscriber(IPricingLifecycleSubscriber):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
    ) -> None:
        self._consumer = consumer

        self._current_message: ConsumerRecord | None = None

    async def subscribe(self) -> AsyncIterator[RfqPricingLifecycleEvent]:
        await self._consumer.start()

        try:
            async for message in self._consumer:
                self._current_message = message

                payload: dict[str, Any] = json.loads(
                    message.value.decode("utf-8"),
                )

                yield self._deserialize(payload)

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
        payload: dict[str, Any],
    ) -> RfqPricingLifecycleEvent:
        event_type = payload["type"]
        request_id = payload["rfq_id"]

        match event_type:
            case "rfq_pricing_activated":
                return PricingActivated(
                    request_id=request_id,
                    revision=payload["revision"],
                )

            case "rfq_pricing_changed":
                return PricingChanged(
                    request_id=request_id,
                    revision=payload["revision"],
                )

            case "rfq_pricing_stopped":
                return PricingStopped(
                    request_id=request_id,
                )

            case _:
                raise ValueError(
                    f"Unknown lifecycle event: {event_type}"
                )
                print(
                    f"[WARNING] "
                    f"Unsupported lifecycle event: {event_type}."
                )