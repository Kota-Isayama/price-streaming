from dataclasses import dataclass

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.pricing.model.pricing_shard import PricingShardId




@dataclass(frozen=True, slots=True)
class RfqDomainEventDeliveryId:
    """
    Brokerから受け取った「この1 delivery」を識別するApplication側のID。

    Kafka offsetそのものではない。
    RabbitMQ delivery tagそのものでもない。

    Infrastructure Adapter内部で、

        delivery_id
            ↓
        Kafka topic/partition/offset

    の対応を保持する。
    """

    value: str


@dataclass(frozen=True, slots=True)
class RfqDomainEventDelivery:
    """
    Pricing Workerが受け取る純粋なデータ。

    重要:
    - ConsumerRecordを持たない
    - AIOKafkaConsumerを持たない
    - ack()メソッドを持たない

    ModelはPortを知らない。
    """

    delivery_id: RfqDomainEventDeliveryId
    shard_id: PricingShardId
    event: RfqRegisteredIntegrationEvent