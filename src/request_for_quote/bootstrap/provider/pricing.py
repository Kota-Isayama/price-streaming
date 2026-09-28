import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

import aio_pika
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
import redis.asyncio as redis

from request_for_quote.application.port.pricing_update_publisher import (
    IPricingUpdatePublisher,
)
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver
from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership
from request_for_quote.bootstrap.settings.pricing_events import KafkaPricingEventsSettings, PricingEventsSettings, RabbitMqQuorumPricingEventsSettings
from request_for_quote.bootstrap.settings.pricing_worker import (
    KafkaRfqDomainEventsSubscriberSettings,
    RedisPricingUpdatesPublisherSettings,
    ZeroMqMarketDataSettings,
)
from request_for_quote.infrastructure.application.adapter.market_data_subscriber.zeromq_market_data_subscriber import ZeroMqMarketDataSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_update_publisher.redis_pricing_update_publisher import RedisPricingUpdatePublisher
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_partition_router import KafkaPricingPartitionRouter
from request_for_quote.infrastructure.application.pricing.adapter.pricing_rfq_domain_events_subscriber.kafka.pricing_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.pricing.adapter.pricing_rfq_domain_events_subscriber.rabbitmq.pricing_rfq_domain_events_subscriber import RabbitMqQuorumRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shard_resolver.kafka.pricing_shard_resolver import KafkaPricingShardResolver
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shard_resolver.rabbitmq.pricins_shard_resolver import StablePricingShardResolver
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shared_ownership.kafka.pricing_shared_ownership import KafkaPricingShardOwnership
from request_for_quote.infrastructure.application.pricing.adapter.pricing_shared_ownership.postgres.postgres_pricing_shared_ownership import PostgresPricingShardOwnership
from request_for_quote.infrastructure.postgres.base import AsyncSessionMaker
from request_for_quote.infrastructure.rabbitmq.topology import declare_pricing_topology


@dataclass(frozen=True)
class PricingEventsBackend:
    """
    Pricing Workerから見たRFQ Domain Event Backend一式。
    
    backendが何であってもRuntimeに必要なのはこの3つ。
    
    Subscriber:
        shardのeventを読む
    Ownership:
        どのshardをこのworkerが所有しているか
    ShardResolver:
        request_idがどのshardに属するか。
    """
    subscriber: IPricingRfqDomainEventsSubscriber
    ownership: IPricingShardOwnership
    shard_resolver: IPricingShardResolver


@asynccontextmanager
async def provide_pricing_events_backend(
    *,
    settings: PricingEventsSettings,
    session_maker: AsyncSessionMaker,
) -> AsyncGenerator[PricingEventsBackend]:

    match settings:

        case KafkaPricingEventsSettings():
            async with provide_kafka_pricing_events_backend(
                settings
            ) as backend:
                yield backend

        case RabbitMqQuorumPricingEventsSettings():
            async with (
                provide_rabbitmq_quorum_pricing_events_backend(
                    settings=settings,
                    session_maker=session_maker,
                )
            ) as backend:
                yield backend

        case _:
            raise TypeError(
                "Unsupported pricing events backend: "
                f"{type(settings).__name__}"
            )


@asynccontextmanager
async def provide_kafka_pricing_events_backend(
    settings: KafkaPricingEventsSettings,
) -> AsyncGenerator[PricingEventsBackend]:
    """
    Kafkaに関係する部品を一箇所で組み立てる。

                AIOKafkaConsumer
                  /          \
                 /            \
                ▼              ▼
          Subscriber       Ownership
                               │
                     rebalance listener

    このownership instanceとlistener instanceが
    必ず同じになることが重要。
    """

    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
    )

    await consumer.start()

    dispatcher_task: asyncio.Task[None] | None = None

    try:
        # -------------------------------------------------
        # 1. Ownershipを作る
        # -------------------------------------------------
        #
        # Kafka Consumer Groupのpartition assignmentを
        # PricingShard ownershipに変換する。
        #
        ownership = KafkaPricingShardOwnership(
            topic=settings.topic,
        )

        # -------------------------------------------------
        # 2. Subscriberを作る
        # -------------------------------------------------
        #
        # 同じconsumerを読む。
        #
        subscriber = KafkaRfqDomainEventsSubscriber(
            consumer=consumer,
        )

        # -------------------------------------------------
        # 3. 同じownership instanceをlistener登録
        # -------------------------------------------------
        #
        # Kafka
        #   ↓
        # on_partitions_assigned/revoked
        #   ↓
        # ownership
        #   ↓
        # Application Runtime
        #
        consumer.subscribe(
            topics=[settings.topic],
            listener=ownership,
        )

        # -------------------------------------------------
        # 4. 実際のpartition一覧をKafkaから取得
        # -------------------------------------------------
        #
        # settings.partitions のように設定ファイルへ
        # 重複して持たせない。
        #
        partitions = set(range(settings.partition_count))

        # -------------------------------------------------
        # 5. request_id -> PricingShardId
        # -------------------------------------------------
        resolver = KafkaPricingShardResolver(
            router=KafkaPricingPartitionRouter(),
            partitions=set(partitions),
        )

        # -------------------------------------------------
        # 6. Kafka dispatcher開始
        # -------------------------------------------------
        #
        # AIOKafkaConsumer
        #      ↓
        # subscriber.run()
        #      ↓
        # partition別asyncio.Queue
        #      ↓
        # subscribe(shard_id)
        #
        dispatcher_task = asyncio.create_task(
            subscriber.run(),
            name="kafka-rfq-domain-event-dispatcher",
        )

        yield PricingEventsBackend(
            subscriber=subscriber,
            ownership=ownership,
            shard_resolver=resolver,
        )

    finally:
        if dispatcher_task is not None:
            dispatcher_task.cancel()

            await asyncio.gather(
                dispatcher_task,
                return_exceptions=True,
            )

        await consumer.stop()


@asynccontextmanager
async def provide_rabbitmq_quorum_pricing_events_backend(
    *,
    settings: RabbitMqQuorumPricingEventsSettings,
    session_maker: AsyncSessionMaker,
) -> AsyncGenerator[PricingEventsBackend]:
    """
    RabbitMQ Quorum版。

    Kafka版:

        Consumer
         ├─ Subscriber
         └─ Ownership

    RabbitMQ版:

        RabbitMQ
            ↓
        Subscriber

        Postgres
            ↓
        Ownership

    実装元は違うが、
    Pricing Workerから見るinterfaceは同じ。
    """

    connection = await aio_pika.connect_robust(
        settings.url
    )

    try:
        channel = await connection.channel()

        # shard内逐次処理なので最初は1。
        await channel.set_qos(
            prefetch_count=1
        )

        _, queues = await declare_pricing_topology(
            channel=channel,
            exchange_name=settings.exchange,
            queue_prefix=settings.queue_prefix,
            shard_count=settings.shard_count,
        )

        subscriber = (
            RabbitMqQuorumRfqDomainEventsSubscriber(
                queues=queues,
            )
        )

        ownership = (
            PostgresPricingShardOwnership(
                session_maker=session_maker,
                worker_id=settings.worker_id,
                shard_count=settings.shard_count,
                lease_seconds=settings.lease_seconds,
                renew_interval_seconds=(
                    settings.renew_interval_seconds
                ),
                max_shards_per_worker=(
                    settings.max_shards_per_worker
                ),
            )
        )

        resolver = StablePricingShardResolver(
            shard_count=settings.shard_count,
        )

        yield PricingEventsBackend(
            subscriber=subscriber,
            ownership=ownership,
            shard_resolver=resolver,
        )

    finally:
        await connection.close()

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
