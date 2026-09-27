from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, Field

from request_for_quote.bootstrap.settings.base import (
    YamlSettings,
    yaml_settings_config,
)
from request_for_quote.bootstrap.settings.database import DatabaseSettings


class KafkaRfqDomainEventsSubscriberSettings(BaseModel):
    backend: Literal["kafka"]

    bootstrap_servers: str
    topic: str
    consumer_group: str

    partitions: int


# RabbitMQ版はSession ownershipの設計を決めてから追加
RfqDomainEventSettings = Annotated[
    KafkaRfqDomainEventsSubscriberSettings,
    Field(discriminator="backend"),
]


class ZeroMqMarketDataSettings(BaseModel):
    backend: Literal["zeromq"] = "zeromq"

    endpoint: str
    topic: str = ""


class RedisPricingUpdatesPublisherSettings(BaseModel):
    backend: Literal["redis"] = "redis"

    url: str
    channel_prefix: str = "pricing"


class PricingWorkerSettings(YamlSettings):
    database: DatabaseSettings

    rfq_domain_event: RfqDomainEventSettings
    market_data: ZeroMqMarketDataSettings
    pricing_updates: RedisPricingUpdatesPublisherSettings

    model_config = yaml_settings_config(
        "config/pricing-worker.yaml"
    )

    @classmethod
    def load(cls) -> Self:
        empty: dict[str, Any] = {}
        return cls(**empty)
