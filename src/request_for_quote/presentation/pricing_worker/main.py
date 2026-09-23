"""複数のInbound Adapterを起動するProcess Host/Conposition Root

入力は現在のところ以下の２系統が考えられる
┌──────────────────────────────────┐
│                                  │
│ ZeroMQ Market Data Consumer      │
│      ↓                           │
│ HandleMarketDataUpdateUseCase    │
│                                  │
│ RFQ Domain Event Consumer        │
│      ↓                           │
│ ActivateRfqPricingUseCase        │
│ StopRfqPricingUseCase            │
│                                  │
└──────────────────────────────────┘

                           外部世界

ZeroMQ Market Data
        │
        ▼
ZeroMqMarketDataSubscriber
        │
        ▼
HandleMarketDataUpdateUseCase


Kafka rfq-domain-events
        │
        ▼
KafkaRfqDomainEventsSubscriber
        │
        ├─ RfqRegistered
        │       ↓
        │  HandleRfqRegisteredForPricingUseCase
        │
        ├─ RfqChanged
        │       ↓
        │  HandleRfqChangedForPricingUseCase
        │
        └─ RfqStopped
                ↓
           HandleRfqStoppedForPricingUseCase
このように、実行時には、MQなどを通じて入力されるイベントに対して一つのUseCaseが実行される。

構造は通常のWebAPIと全く同様である。
                   外部世界

 HTTP ──────→ FastAPI Endpoint ──┐
                                 │
 CLI ───────→ CLI Handler ───────┤
                                 ▼
 ZeroMQ ─────→ Market Handler → Application
                                 ▲
 Kafka ──────→ RFQ Handler ──────┘
"""


"""Pricing Worker Process Host / Composition Root.

Inbound adapters:

    ZeroMQ Market Data
            ↓
    HandleMarketDataUpdateUseCase

    Kafka RFQ Domain Events
            ↓
    RfqRegisteredIntegrationEvent
            ↓
    HandleRfqRegisteredForPricingUseCase

    RfqChangedIntegrationEvent
            ↓
    HandleRfqChangedForPricingUseCase

    RfqStoppedIntegrationEvent
            ↓
    HandleRfqStoppedForPricingUseCase
"""

import asyncio

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

from request_for_quote.application.integration.events.rfq import (
    RfqRegisteredIntegrationEvent,
)
from request_for_quote.application.port.rfq_domain_events_subscriber import (
    IRfqDomainEventsSubscriber,
)
from request_for_quote.application.pricing.use_case.handle_rfq_registered import (
    HandleRfqRegisteredForPricingUseCase,
)
from request_for_quote.application.pricing.use_case.handle_market_data_update import (
    HandleMarketDataUpdateUseCase,
)

from request_for_quote.domain.market.market import MarketState
from request_for_quote.domain.pricing.swap_pricer import SwapPricer

from request_for_quote.infrastructure.application.adapter.market_data_subscriber.zeromq_market_data_subscriber import (
    ZeroMqMarketDataSubscriber,
)

from request_for_quote.infrastructure.application.adapter.pricing_session_store.in_memory_pricing_session_store import (
    InMemoryPricingSessionRegistry,
)
from request_for_quote.infrastructure.application.adapter.pricing_update_publisher.redis_pricing_update_publisher import (
    RedisPricingUpdatePublisher,
)
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.kafka_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_partition_router import KafkaPricingPartitionRouter
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_rebalance_listener import PricingRebalanceListener
from request_for_quote.infrastructure.application.pricing.adapter.sql_alchemy_pricing_session_unit_of_work import (
    SqlAlchemyPricingSessionUnitOfWork,
)
from request_for_quote.infrastructure.postgres.base import (
    create_engine,
    create_session_maker,
)


DATABASE_URL = (
    "postgresql+asyncpg://"
    "postgres:postgres@localhost:5432/request_for_quote"
)

RFQ_DOMAIN_EVENTS_TOPIC = "rfq-domain-events"

PRICING_REDIS_URL = "redis://localhost:6379"


async def consume_market_data(
    subscriber: ZeroMqMarketDataSubscriber,
    use_case: HandleMarketDataUpdateUseCase,
) -> None:
    async for update in subscriber.subscribe():
        await use_case.execute(
            market_data_id=update.market_data_id,
            value=update.value,
        )


async def consume_rfq_domain_events(
    subscriber: IRfqDomainEventsSubscriber,
    handle_registered: HandleRfqRegisteredForPricingUseCase,
) -> None:
    async for event in subscriber.subscribe():

        match event:
            case RfqRegisteredIntegrationEvent():
                await handle_registered.execute(event)

            case _:
                raise ValueError(
                    "Unsupported RFQ domain event: "
                    f"{type(event).__name__}"
                )

        # UseCaseが正常に完了した場合のみoffsetをcommit
        await subscriber.ack()


async def main() -> None:
    #
    # Shared process state
    #

    market_state = MarketState()

    session_registry = (
        InMemoryPricingSessionRegistry()
    )

    #
    # Infrastructure resources
    #

    engine = create_engine(
        DATABASE_URL
    )

    session_maker = create_session_maker(
        engine
    )

    redis_client = redis.Redis.from_url(
        PRICING_REDIS_URL,
        decode_responses=True,
    )

    #
    # UoW factory
    #
    # UoWそのものは共有しない。
    # 1 application operationごとに新しく生成する。
    #

    pricing_session_uow_factory = (
        lambda: SqlAlchemyPricingSessionUnitOfWork(
            session_maker
        )
    )

    pricing_update_publisher = (
        RedisPricingUpdatePublisher(
            redis_client
        )
    )

    #
    # Domain services
    #

    pricer = SwapPricer()

    #
    # Application UseCases
    #

    handle_rfq_registered = (
        HandleRfqRegisteredForPricingUseCase(
            pricing_session_uow_factory=(
                pricing_session_uow_factory
            ),
            session_registry=session_registry,
            market_state=market_state,
            pricer=pricer,
            pricing_update_publisher=(
                pricing_update_publisher
            ),
        )
    )

    handle_market_data_update = (
        HandleMarketDataUpdateUseCase(
            session_store=session_registry,
            market_state=market_state,
            pricer=pricer,
            pricing_update_publisher=(
                pricing_update_publisher
            ),
        )
    )

    #
    # Inbound adapters
    #

    market_data_subscriber = (
        ZeroMqMarketDataSubscriber(
            endpoint="tcp://127.0.0.1:5555",
            topics={
                "JPY-OIS",
            },
        )
    )

    kafka_consumer = AIOKafkaConsumer(
        RFQ_DOMAIN_EVENTS_TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="pricing-workers",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        session_timeout_ms=6000,
        heartbeat_interval_ms=2000,
    )

    partition_router = (
        KafkaPricingPartitionRouter()
    )

    rebalance_listener = (
        PricingRebalanceListener(
            topic=RFQ_DOMAIN_EVENTS_TOPIC,
            all_partitions={
                0,
                1,
                2,
                3,
            },
            session_uow_factory=(
                pricing_session_uow_factory
            ),
            session_registry=session_registry,
            partition_router=partition_router,
        )
    )

    kafka_consumer.subscribe(
        topics=[
            RFQ_DOMAIN_EVENTS_TOPIC,
        ],
        listener=rebalance_listener,
    )

    rfq_domain_events_subscriber = (
        KafkaRfqDomainEventsSubscriber(
            consumer=kafka_consumer,
        )
    )

    #
    # Process lifecycle
    #

    await kafka_consumer.start()

    try:
        async with asyncio.TaskGroup() as task_group:

            task_group.create_task(
                consume_market_data(
                    subscriber=market_data_subscriber,
                    use_case=(
                        handle_market_data_update
                    ),
                )
            )

            task_group.create_task(
                consume_rfq_domain_events(
                    subscriber=(
                        rfq_domain_events_subscriber
                    ),
                    handle_registered=(
                        handle_rfq_registered
                    ),
                )
            )

    finally:
        await kafka_consumer.stop()
        await redis_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())