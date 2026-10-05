from typing import Annotated, Literal

from pydantic import BaseModel, Field

from request_for_quote.infrastructure.application.pricing.adapter.pricing_rfq_domain_events_subscriber.rabbitmq_super_stream.pricing_rfq_domain_events_subscriber import RabbitMqSuperStreamPricingRfqDomainEventsSubscriber


class KafkaPricingEventsSettings(BaseModel):
    backend: Literal["kafka"]

    bootstrap_servers: str
    topic: str
    partition_count: int
    consumer_group: str


class RabbitMqQuorumPricingEventsSettings(BaseModel):
    backend: Literal["rabbitmq_quorum"]

    url: str

    exchange: str
    queue_prefix: str
    shard_count: int

    worker_id: str

    lease_seconds: int 
    renew_interval_seconds: float
    max_shards_per_worker: int


class RabbitMqSuperStreamPricingEventsSettings(BaseModel):
    backend: Literal["rabbitmq_super_stream"]

    # AMQP 0-9-1
    # Super Stream partitionを共通exchangeへbindするために使用
    amqp_url: str
    exchange: str = "rfq.pricing"

    # RabbitMQ Stream Protocol
    host: str
    port: int = 5552

    vhost: str
    username: str
    password: str

    super_stream: str
    consumer_group: str

    partition_count: int


PricingEventsSettings = Annotated[
    KafkaPricingEventsSettings
    | RabbitMqQuorumPricingEventsSettings
    | RabbitMqSuperStreamPricingEventsSettings,
    Field(discriminator="backend"),
]
