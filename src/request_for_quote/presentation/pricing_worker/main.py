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
from request_for_quote.application.port.market_data_subscriber import IMarketDataSubscriber
from request_for_quote.application.port.rfq_domain_events_subscriber import (
    IRfqDomainEventsSubscriber,
)
from request_for_quote.application.pricing.use_case.handle_rfq_registered import (
    HandleRfqRegisteredForPricingUseCase,
)
from request_for_quote.application.pricing.use_case.handle_market_data_update import (
    HandleMarketDataUpdateUseCase,
)

from request_for_quote.bootstrap.processes.pricing_worker import bootstrap_pricing_worker
from request_for_quote.bootstrap.settings.pricing_worker import PricingWorkerSettings
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


async def consume_market_data(
    subscriber: IMarketDataSubscriber,
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
    settings = PricingWorkerSettings.load()

    async with bootstrap_pricing_worker(settings=settings) as worker_container:
        market_state = MarketState()
        #
        # UoW factory
        #
        # UoWそのものは共有しない。
        # 1 application operationごとに新しく生成する。
        #
        pricing_session_uow_factory = (
            lambda: SqlAlchemyPricingSessionUnitOfWork(
                worker_container.session_maker,
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
                session_registry=worker_container.session_registry,
                market_state=market_state,
                pricer=pricer,
                pricing_update_publisher=worker_container.pricing_updates,
            )
        )

        handle_market_data_update = (
            HandleMarketDataUpdateUseCase(
                session_store=worker_container.session_registry,
                market_state=market_state,
                pricer=pricer,
                pricing_update_publisher=worker_container.pricing_updates,
            )
        )

        async with asyncio.TaskGroup() as task_group:

            task_group.create_task(
                consume_market_data(
                    subscriber=worker_container.market_data_subscriber,
                    use_case=(
                        handle_market_data_update
                    ),
                )
            )

            task_group.create_task(
                consume_rfq_domain_events(
                    subscriber=worker_container.rfq_domain_events_subscriber,
                    handle_registered=handle_rfq_registered,
                )
            )



if __name__ == "__main__":
    asyncio.run(main())