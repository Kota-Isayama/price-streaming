from re import S
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class KafkaRfqEventsPublisherSettings(BaseModel):
    backend: Literal["kafka"]
    bootstrap_servers: str
    topic: str


class RabbitMqRfqEventsPublisherSettings(BaseModel):
    backend: Literal["rabbitmq_quorum"]

    url: str
    exchange: str

    pricing_shard_count: int


RfqEventsPublisherSettings = Annotated[
    KafkaRfqEventsPublisherSettings
    | RabbitMqRfqEventsPublisherSettings,
    Field(discriminator="backend"),
]


class KafkaRfqEventsSubscriberSettings(BaseModel):
    backend: Literal["kafka"]

    bootstrap_servers: str
    topic: str
    group_id: str


class RabbitMqRfqEventsSubscriberSettings(BaseModel):
    backend: Literal["rabbitmq_quorum"]

    url: str = "amqp://rfq:rfq@localhost:5672/rfq"

    exchange: str = "rfq.pricing"
    queue_prefix: str = "rfq.pricing"

    shared_count: int = 4

    prefetch_count: int = 10

    # Coordinator
    worker_id: str 
    lease_seconds: int = 10
    renew_interval_seconds: float = 3.0

    # 最初の実装では1 workerが取りすぎないように上限を置く
    max_shards_per_worker: int = 2


RfqEventsSubscriberSettings = Annotated[
    KafkaRfqEventsSubscriberSettings
    | RabbitMqRfqEventsSubscriberSettings,
    Field(discriminator="backend"),
]
