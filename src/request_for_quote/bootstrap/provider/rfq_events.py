from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aio_pika
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from request_for_quote.application.port.integration_event_publisher import (
    IIntegrationEventPublisher,
)
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber
from request_for_quote.bootstrap.settings.rfq_events import (
    KafkaRfqEventsPublisherSettings,
    KafkaRfqEventsSubscriberSettings,
    RabbitMqRfqEventsPublisherSettings,
    RabbitMqRfqEventsSubscriberSettings,
    RfqEventsPublisherSettings,
    RfqEventsSubscriberSettings,
)
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.event_router import (
    KafkaEventRouter,
)
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.kafka_integration_event_publisher import (
    KafkaIntegrationEventPublisher,
)

from request_for_quote.infrastructure.application.adapter.integration_event_publisher.rabbitmq.rabbitmq_integration_event_publisher import RabbitMqIntegrationEventPublisher
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.kafka_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.rabbitmq.rabbitmq_rfq_domain_events_publisher import RabbitmqRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.pricing.adapter.pricing_rfq_domain_events_subscriber.rabbitmq.pricing_rfq_domain_events_subscriber import RabbitMqQuorumRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shard_resolver.rabbitmq.pricins_shard_resolver import StablePricingShardResolver
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shared_ownership.postgres.postgres_pricing_shared_ownership import PostgresPricingShardOwnership
from request_for_quote.infrastructure.rabbitmq.topology import declare_pricing_topology


@asynccontextmanager
async def provide_rfq_events_publisher(
    settings: RfqEventsPublisherSettings,
) -> AsyncGenerator[IIntegrationEventPublisher]:

    match settings:
        case KafkaRfqEventsPublisherSettings():
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.bootstrap_servers,
            )

            await producer.start()

            try:
                yield KafkaIntegrationEventPublisher(
                    producer=producer,
                    router=KafkaEventRouter(),
                )
            finally:
                await producer.stop()

        case RabbitMqRfqEventsPublisherSettings():
            connection = await aio_pika.connect_robust(
                settings.url,
            )

            try:
                channel = await connection.channel(
                    publisher_confirms=True,
                    on_return_raises=True,
                )

                exchange = await channel.declare_exchange(
                    settings.exchange,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )

                resolver = StablePricingShardResolver(
                    shard_count=settings.pricing_shard_count,
                )

                yield RabbitMqIntegrationEventPublisher(
                    exchange=exchange,
                    shard_resolver=resolver,
                )

            finally:
                await connection.close()


@asynccontextmanager
async def provide_rfq_events_subscriber(
    settings: RfqEventsSubscriberSettings,
) -> AsyncGenerator[IRfqDomainEventsSubscriber]:

    match settings:
        case KafkaRfqEventsSubscriberSettings():
            consumer = AIOKafkaConsumer(
                settings.topic,
                bootstrap_servers=settings.bootstrap_servers,
                group_id=settings.group_id,
                enable_auto_commit=False,
            )

            await consumer.start()

            try:
                yield KafkaRfqDomainEventsSubscriber(
                    consumer=consumer,
                )
            finally:
                await consumer.stop()

        case RabbitMqRfqEventsSubscriberSettings():
            connection = await aio_pika.connect_robust(
                settings.url,
            )

            try:
                channel = await connection.channel()

                await channel.set_qos(
                    prefetch_count=settings.prefetch_count,
                )

                exchange = await channel.declare_exchange(
                    settings.exchange,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )

                queue = await channel.declare_queue(
                    settings.queue,
                    durable=True,
                )

                for routing_key in settings.routing_keys:
                    await queue.bind(
                        exchange,
                        routing_key=routing_key,
                    )

                yield RabbitmqRfqDomainEventsSubscriber(
                    queue=queue,
                )

            finally:
                await connection.close()
