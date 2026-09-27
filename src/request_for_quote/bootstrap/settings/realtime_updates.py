from typing import Literal

from pydantic import BaseModel


class RedisNotificationUpdatesPublisherSettings(BaseModel):
    backend: Literal["redis"]

    url: str
    channel_prefix: str


class RedisNotificationUpdatesSubscriberSettings(BaseModel):
    backend: Literal["redis"]

    url: str
    channel_prefix: str


class RedisPricingUpdatesPublisherSettings(BaseModel):
    backend: Literal["redis"] = "redis"

    url: str
    channel_prefix: str


class RedisPricingUpdatesSubscriberSettings(BaseModel):
    backend: Literal["redis"] = "redis"

    url: str
    channel_prefix: str
