# bootstrap/processes/pricing_worker.py

from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.port.market_data_subscriber import IMarketDataSubscriber
from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver
from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership
from request_for_quote.bootstrap.provider.database import (
    provide_session_maker,
)
from request_for_quote.bootstrap.provider.pricing import (
    provide_market_data_subscriber,
    provide_pricing_events_backend,
    provide_pricing_updates_publisher,
)
from request_for_quote.bootstrap.settings.pricing_worker import (
    PricingWorkerSettings,
)
from request_for_quote.infrastructure.application.adapter.pricing_session_store.in_memory_pricing_session_store import InMemoryPricingSessionRegistry
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_partition_router import KafkaPricingPartitionRouter
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_rebalance_listener import PricingRebalanceListener
from request_for_quote.infrastructure.application.pricing.adapter.sql_alchemy_pricing_session_unit_of_work import SqlAlchemyPricingSessionUnitOfWork


@dataclass(frozen=True)
class PricingWorkerContainer:
    session_maker: async_sessionmaker[AsyncSession]

    market_data_subscriber: IMarketDataSubscriber
    pricing_updates: IPricingUpdatePublisher

    session_registry: IPricingSessionRegistry

    rfq_domain_events_subscriber: IPricingRfqDomainEventsSubscriber

    pricing_shard_ownership: IPricingShardOwnership

    pricing_shard_resolver: IPricingShardResolver


@asynccontextmanager
async def bootstrap_pricing_worker(
    settings: PricingWorkerSettings,
) -> AsyncGenerator[PricingWorkerContainer]:

    async with AsyncExitStack() as stack:

        session_maker = await stack.enter_async_context(
            provide_session_maker(
                settings.database,
            )
        )

        session_registry = (
            InMemoryPricingSessionRegistry()
        )

        # ==========================================
        # RFQ Domain Event Backend
        #
        # Kafkaなら:
        #   Kafka Subscriber
        #   Kafka ownership
        #   Kafka resolver
        #
        # RabbitMQなら:
        #   RabbitMQ Subscriber
        #   Postgres ownership
        #   Stable resolver
        #
        # をまとめて返す。
        # ==========================================
        backend = await stack.enter_async_context(
            provide_pricing_events_backend(
                settings=settings.rfq_domain_event,
                session_maker=session_maker,
            )
        )

        market_data_subscriber = await stack.enter_async_context(
            provide_market_data_subscriber(
                settings=settings.market_data,
            )
        )

        pricing_updates = (
            await stack.enter_async_context(
                provide_pricing_updates_publisher(
                    settings.pricing_updates,
                )
            )
        )

        yield PricingWorkerContainer(
            session_maker=session_maker,

            market_data_subscriber=(
                market_data_subscriber
            ),
            pricing_updates=pricing_updates,
            session_registry=session_registry,

            rfq_domain_events_subscriber=(
                backend.subscriber
            ),
            pricing_shard_ownership=(
                backend.ownership
            ),
            pricing_shard_resolver=(
                backend.shard_resolver
            ),
        )