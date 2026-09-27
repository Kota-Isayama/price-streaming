from dataclasses import dataclass


@dataclass(frozen=True)
class PricingShardId:
    """
    Pricing Worker内部で使う「論理Shard」。

    Kafkaでは:
        PricingShardId(2) == Kafka partition 2

    RabbitMQ Super Streamでは:
        PricingShardId(2) == stream partition 2

    RabbitMQ Quorum Queue版では:
        PricingShardId(2) == pricing-shard-2 queue

    Application側は、これがKafka partitionなのかRabbitMQ queueなのかを知らない。
    """
    value: int
