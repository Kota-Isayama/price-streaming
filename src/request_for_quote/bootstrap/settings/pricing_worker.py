from dataclasses import dataclass
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, Field

from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver
from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership
from request_for_quote.bootstrap.settings.base import (
    YamlSettings,
    yaml_settings_config,
)
from request_for_quote.bootstrap.settings.database import DatabaseSettings
from request_for_quote.bootstrap.settings.pricing_events import PricingEventsSettings



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

    rfq_domain_event: PricingEventsSettings
    market_data: ZeroMqMarketDataSettings
    pricing_updates: RedisPricingUpdatesPublisherSettings

    model_config = yaml_settings_config(
        "config/pricing-worker.yaml"
    )

    @classmethod
    def load(cls) -> Self:
        empty: dict[str, Any] = {}
        return cls(**empty)
