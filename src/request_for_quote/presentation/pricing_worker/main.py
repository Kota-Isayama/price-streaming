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
from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.model.pricing_shard_ownership_change import PricingShardAcquired, PricingShardReleased
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber
from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership
from request_for_quote.application.pricing.use_case.handle_rfq_registered import (
    HandleRfqRegisteredForPricingUseCase,
)
from request_for_quote.application.pricing.use_case.handle_market_data_update import (
    HandleMarketDataUpdateUseCase,
)

from request_for_quote.application.pricing.use_case.release_pricing_shard import ReleasePricingShardUseCase
from request_for_quote.application.pricing.use_case.restore_pricing_shard import RestorePricingShardUseCase
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


async def run_owned_pricing_shards(
    ownership: IPricingShardOwnership,
    subscriber: IPricingRfqDomainEventsSubscriber,
    restore_shard: RestorePricingShardUseCase,
    release_shard: ReleasePricingShardUseCase,
    handle_registered: HandleRfqRegisteredForPricingUseCase,
) -> None:
    """
    Pricing WorkerのShard lifecycleを管理する。

    例:

        Kafka rebalance
            ↓
        ACQUIRED shard-2
            ↓
        restore Session
            ↓
        shard-2 consumer Task開始
            ↓
        ownership.complete()
            ↓
        Kafka callback return


    RELEASEDの場合:

        Kafka rebalance
            ↓
        RELEASED shard-2
            ↓
        shard-2 Task cancel
            ↓
        Task停止完了を待つ
            ↓
        Session release
            ↓
        ownership.complete()
            ↓
        Kafka callback return
    """
    shard_tasks: dict[PricingShardId, asyncio.Task[None]] = {}

    # TaskGroupを使うことで
    # どれか一つのshard consumerが異常終了したら
    # Worker全体も以上として扱える
    async with asyncio.TaskGroup() as task_group:
        async for change in ownership.changes():
            match change:
                case PricingShardAcquired(shard_id=change.shard_id):
                    if change.shard_id in shard_tasks:
                        raise RuntimeError(
                            "Shard already acquired: "
                            f"{change.shard_id.value}"
                        )
                    # まずsessionを復元する
                    #
                    # Domain Eventを処理し始める前に
                    # stateを準備する。
                    await restore_shard.execute(change.shard_id)

                    # このTaskは長時間生存する
                    #
                    # await consume...としてしまうとownership loopがここで永遠に止まるため、create_taskする必要がある
                    task = task_group.create_task(
                        consume_rfq_domain_events_for_pricing_shard(
                            shard_id=change.shard_id,
                            subscriber=subscriber,
                            handle_registered=handle_registered,
                        ),
                        name=f"rfq-domain-events-shard-{change.shard_id.value}",
                    )

                    shard_tasks[change.shard_id] = task

                    print(
                        "[SHARD ACQUIRED] "
                        f"shard={change.shard_id.value}"
                    )

                case PricingShardReleased(shard_id=change.shard_id):
                    task = shard_tasks.pop(
                        change.shard_id,
                        None,
                    )

                    if task is not None:
                        # 停止を要求
                        task.cancel()

                        try:
                            # cancel要求だけではなく
                            # 本当にTask終了するまで待つ
                            await task
                        except asyncio.CancelledError:
                            pass

                    # consumerが完全に止まった後で InMemory SessionをRelease
                    release_shard.execute(shard_id=change.shard_id)

                    print(
                        "[SHARD RELEASED] "
                        f"shard={change.shard_id.value}"
                    )

            # ここまで終わったら、
            # Kafka callback側のFutureを完了させる。
            #
            # Kafkaはこれを待っている。
            await ownership.complete(change)


async def consume_rfq_domain_events_for_pricing_shard(
    shard_id: PricingShardId,
    subscriber: IPricingRfqDomainEventsSubscriber,
    handle_registered: HandleRfqRegisteredForPricingUseCase,
) -> None:
    """
    1 shard専用の逐次処理Task。

    例えば shard=2 なら、

        Queue[2]
            ↓
        event A
            ↓
        handle A
            ↓
        ack A
            ↓
        event B
            ↓
        handle B
            ↓
        ack B

    となる。

    同じshard内でA/Bを同時に処理しない。"""
    async for delivery in subscriber.subscribe(shard_id):
        event = delivery.event

        match event:
            case RfqRegisteredIntegrationEvent():
                await handle_registered.execute(event)
            case _:
                raise ValueError(
                    "Unsupported RFQ domain event: "
                    f"{type(event).__name__}"
                )

        # UseCaseが正常に完了した場合だけAck
        #
        # Kafka版ではここでoffset commit
        await subscriber.ack(delivery)


async def main() -> None:
    settings = PricingWorkerSettings.load()

    async with bootstrap_pricing_worker(
        settings=settings
    ) as worker_container:

        market_state = MarketState()

        # UoWはoperationごとに作る。
        pricing_session_uow_factory = (
            lambda: SqlAlchemyPricingSessionUnitOfWork(
                worker_container.session_maker
            )
        )

        pricer = SwapPricer()

        handle_rfq_registered = (
            HandleRfqRegisteredForPricingUseCase(
                pricing_session_uow_factory=(
                    pricing_session_uow_factory
                ),
                session_registry=(
                    worker_container
                    .session_registry
                ),
                market_state=market_state,
                pricer=pricer,
                pricing_update_publisher=(
                    worker_container
                    .pricing_updates
                ),
            )
        )

        handle_market_data_update = (
            HandleMarketDataUpdateUseCase(
                session_store=(
                    worker_container
                    .session_registry
                ),
                market_state=market_state,
                pricer=pricer,
                pricing_update_publisher=(
                    worker_container
                    .pricing_updates
                ),
            )
        )

        restore_pricing_shard = (
            RestorePricingShardUseCase(
                session_uow_factory=(
                    pricing_session_uow_factory
                ),
                session_registry=(
                    worker_container
                    .session_registry
                ),
                shard_resolver=(
                    worker_container
                    .pricing_shard_resolver
                ),
            )
        )

        release_pricing_shard = (
            ReleasePricingShardUseCase(
                session_registry=(
                    worker_container
                    .session_registry
                ),
                shard_resolver=(
                    worker_container
                    .pricing_shard_resolver
                ),
            )
        )

        async with asyncio.TaskGroup() as tg:

            # Task 1:
            # ZeroMQ Market Dataを永続的にconsume。
            tg.create_task(
                consume_market_data(
                    subscriber=(
                        worker_container
                        .market_data_subscriber
                    ),
                    use_case=(
                        handle_market_data_update
                    ),
                ),
                name="market-data-consumer",
            )

            # Task 2:
            # Kafka ownershipを監視。
            #
            # このTaskの内部からさらに
            # shardごとのconsumer Taskが生成される。
            tg.create_task(
                run_owned_pricing_shards(
                    ownership=(
                        worker_container
                        .pricing_shard_ownership
                    ),
                    subscriber=(
                        worker_container
                        .rfq_domain_events_subscriber
                    ),
                    restore_shard=(
                        restore_pricing_shard
                    ),
                    release_shard=(
                        release_pricing_shard
                    ),
                    handle_registered=(
                        handle_rfq_registered
                    ),
                ),
                name="pricing-shard-runtime",
            )


if __name__ == "__main__":
    asyncio.run(main())