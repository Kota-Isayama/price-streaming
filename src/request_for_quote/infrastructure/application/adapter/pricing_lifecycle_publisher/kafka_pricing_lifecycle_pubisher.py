import json

from aiokafka import AIOKafkaProducer

from request_for_quote.application.port.pricing_lifecycle_publisher import IPricingLifecyclePublisher
from request_for_quote.application.pricing.lifecycle import PricingActivated, PricingChanged, PricingStopped, RfqPricingLifecycleEvent


class KafkaPricingLifecyclePublisher(IPricingLifecyclePublisher):
    def __init__(
        self,
        producer: AIOKafkaProducer,  # producerは外から与えるべき？
        topic: str,
    ) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(
        self,
        event: RfqPricingLifecycleEvent,
    ) -> None:
        key = event.request_id.encode("utf-8")  # encodeが必要なのか調べること

        payload = self._serialize(event)

        await self._producer.send_and_wait(  # sendとの違いは何か？
            self._topic,
            key=key,
            value=json.dumps(payload).encode("utf-8"),
        )

    def _serialize(
        self,
        event: RfqPricingLifecycleEvent,
    ) -> dict:
        match event:
            case PricingActivated():
                return {
                    "type": "rfq_pricing_activated",
                    "rfq_id": event.request_id,
                    "revision": event.revision,
                }
            case PricingChanged():
                return {
                    "type": "rfq_pricing_changed",
                    "rfq_id": event.request_id,
                    "revision": event.revision,
                }
            case PricingStopped():
                return {
                    "type": "rfq_pricing_stopped",
                    "rfq_id": event.request_id,
                }
        raise TypeError(
            f"Unsupported lifecycle event: {type(event)}"
        )
        print(
            "[WARNING] "
            f"Unsupported type event: (event)."
        )
    
