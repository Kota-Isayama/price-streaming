from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
import redis.asyncio as redis

from request_for_quote.application.port.pricing_update_publisher import (
    IPricingUpdatePublisher,
)
from request_for_quote.bootstrap.settings.pricing_worker import (
    KafkaRfqDomainEventsSubscriberSettings,
    RedisPricingUpdatesPublisherSettings,
    ZeroMqMarketDataSettings,
)
from request_for_quote.infrastructure.application.adapter.market_data_subscriber.zeromq_market_data_subscriber import ZeroMqMarketDataSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_update_publisher.redis_pricing_update_publisher import RedisPricingUpdatePublisher
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.kafka_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber


@asynccontextmanager
async def provide_pricing_updates_publisher(
    settings: RedisPricingUpdatesPublisherSettings,
) -> AsyncGenerator[IPricingUpdatePublisher]:
    redis_client = redis.Redis.from_url(
        settings.url,
    )

    try:
        yield RedisPricingUpdatePublisher(
            redis=redis_client,
            channel_prefix=settings.channel_prefix,
        )
    finally:
        await redis_client.aclose()


@asynccontextmanager
async def provide_rfq_domain_events_subscriber(
    settings: KafkaRfqDomainEventsSubscriberSettings,
    rebalance_listener: ConsumerRebalanceListener,
) -> AsyncGenerator[KafkaRfqDomainEventsSubscriber]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
    )

    await consumer.start()

    consumer.subscribe(
        topics=[settings.topic],
        listener=rebalance_listener,
    )

    try:
        yield KafkaRfqDomainEventsSubscriber(
            consumer=consumer,
        )
    finally:
        await consumer.stop()


@asynccontextmanager
async def provide_market_data_subscriber(
    settings: ZeroMqMarketDataSettings,
) -> AsyncGenerator[ZeroMqMarketDataSubscriber]:
    subscriber = ZeroMqMarketDataSubscriber(
        endpoint=settings.endpoint,
        topics={settings.topic},
    )
    
    try:
        yield subscriber
    finally:
        subscriber.close()
