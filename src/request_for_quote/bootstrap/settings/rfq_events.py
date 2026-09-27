from re import S
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class KafkaRfqEventsPublisherSettings(BaseModel):
    backend: Literal["kafka"]
    bootstrap_servers: str
    topic: str


class RabbitMqRfqEventsPublisherSettings(BaseModel):
    backend: Literal["rabbitmq"]

    url: str
    exchange: str


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
    backend: Literal["rabbitmq"]

    url: str

    exchange: str 
    queue: str

    routing_keys: list[str]
    prefetch_count: int


RfqEventsSubscriberSettings = Annotated[
    KafkaRfqEventsSubscriberSettings
    | RabbitMqRfqEventsSubscriberSettings,
    Field(discriminator="backend"),
]
