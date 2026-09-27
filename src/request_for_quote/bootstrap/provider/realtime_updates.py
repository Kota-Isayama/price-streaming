from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dis import findlinestarts
from typing import AsyncGenerator, Callable

from redis.asyncio import Redis
import redis.asyncio as redis

from request_for_quote.application.port.notification_publisher import INotificationPublisher
from request_for_quote.application.port.notification_subscriber import INotificationSubscriber
from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher
from request_for_quote.application.port.pricing_update_subscriber import IPricingUpdateSubscriber
from request_for_quote.bootstrap.settings.realtime_updates import (
    RedisNotificationUpdatesPublisherSettings,
    RedisNotificationUpdatesSubscriberSettings,
    RedisPricingUpdatesPublisherSettings,
    RedisPricingUpdatesSubscriberSettings,
)
from request_for_quote.infrastructure.application.adapter.notification_publisher.redis.redis_notification_publisher import RedisNotificationPublisher
from request_for_quote.infrastructure.application.adapter.notification_subscriber.redis.redis_notification_subscriber import RedisNotificationSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_udpate_subscriber.redis.redis_pricing_update_subscriber import RedisPricingUpdateSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_update_publisher.redis_pricing_update_publisher import RedisPricingUpdatePublisher


@asynccontextmanager
async def provide_notification_updates_publisher(
    settings: RedisNotificationUpdatesPublisherSettings,
) -> AsyncGenerator[INotificationPublisher]:

    redis_client = Redis.from_url(
        settings.url,
    )

    try:
        yield RedisNotificationPublisher(
            redis_client=redis_client,
            channel_prefix=settings.channel_prefix,
        )

    finally:
        await redis_client.aclose()


@asynccontextmanager
async def provide_notification_updates_subscriber(
    settings: RedisNotificationUpdatesSubscriberSettings,
) -> AsyncGenerator[INotificationSubscriber]:
    redis_client = Redis.from_url(
        settings.url,
    )

    try:
        yield RedisNotificationSubscriber(
            redis_client=redis_client,
            channel_prefix=settings.channel_prefix,
        )
    finally:
        await redis_client.aclose()


@asynccontextmanager
async def provide_pricing_updates_subscriber_factory(
    settings: RedisPricingUpdatesSubscriberSettings,
) -> AsyncGenerator[
    Callable[[], IPricingUpdateSubscriber]
]:
    redis_client = redis.Redis.from_url(
        settings.url,
    )

    try:
        def factory() -> IPricingUpdateSubscriber:
            return RedisPricingUpdateSubscriber(
                pubsub=redis_client.pubsub(),
            )

        yield factory

    finally:
        await redis_client.aclose()


@asynccontextmanager
async def provide_pricing_updates_publisher(
    settings: RedisPricingUpdatesPublisherSettings,
) -> AsyncGenerator[IPricingUpdatePublisher]:
    redis = Redis.from_url(
        settings.url,
        decode_responses=False,
    )

    try:
        yield RedisPricingUpdatePublisher(
            redis=redis,
            channel_prefix=settings.channel_prefix,
        )
    finally:
        await redis.aclose()


@asynccontextmanager
async def provide_pricing_updates_subscriber(
    settings: RedisPricingUpdatesSubscriberSettings,
) -> AsyncGenerator[IPricingUpdateSubscriber]:
    redis = Redis.from_url(
        settings.url,
        decode_responses=False,
    )

    try:
        yield RedisPricingUpdateSubscriber(
            pubsub=redis.pubsub(),
        )
    finally:
        await redis.aclose()


@asynccontextmanager
async def provide_notification_updates_subscriber_factory(
    settings: RedisNotificationUpdatesSubscriberSettings,
) -> AsyncGenerator[
    Callable[[], INotificationSubscriber]
]:
    redis_client = redis.Redis.from_url(
        settings.url,
        decode_responses=True,
    )

    try:
        def factory() -> INotificationSubscriber:
            return RedisNotificationSubscriber(
                redis_client=redis_client,
                channel_prefix=settings.channel_prefix,
            )

        yield factory

    finally:
        await redis_client.aclose()