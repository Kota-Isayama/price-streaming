from typing import Annotated, Literal

from pydantic import BaseModel, Field


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


PricingEventsSettings = Annotated[
    KafkaPricingEventsSettings
    | RabbitMqQuorumPricingEventsSettings,
    Field(discriminator="backend"),
]
