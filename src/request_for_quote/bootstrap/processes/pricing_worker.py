# bootstrap/processes/pricing_worker.py

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.port.market_data_subscriber import IMarketDataSubscriber
from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber
from request_for_quote.bootstrap.provider.database import (
    provide_session_maker,
)
from request_for_quote.bootstrap.provider.pricing import (
    provide_market_data_subscriber,
    provide_rfq_domain_events_subscriber,
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

    rfq_domain_events_subscriber: IRfqDomainEventsSubscriber
    market_data_subscriber: IMarketDataSubscriber
    pricing_updates: IPricingUpdatePublisher

    session_registry: IPricingSessionRegistry


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

        session_registry = InMemoryPricingSessionRegistry()

        rebalance_listener = PricingRebalanceListener(
            topic=settings.rfq_domain_event.topic,
            all_partitions={i for i in range(settings.rfq_domain_event.partitions)},  # なんか違うよなぁ...
            session_registry=session_registry,
            session_uow_factory=lambda: SqlAlchemyPricingSessionUnitOfWork(session_maker),
            partition_router=KafkaPricingPartitionRouter(),
        )

        rfq_domain_events_subscriber = await stack.enter_async_context(
            provide_rfq_domain_events_subscriber(
                settings.rfq_domain_event,
                rebalance_listener,
            )
        )

        market_data_subscriber = await stack.enter_async_context(
            provide_market_data_subscriber(
                settings.market_data,
            )
        )

        pricing_updates = await stack.enter_async_context(
            provide_pricing_updates_publisher(
                settings.pricing_updates,
            )
        )

        yield PricingWorkerContainer(
            session_maker=session_maker,
            rfq_domain_events_subscriber=rfq_domain_events_subscriber,
            market_data_subscriber=market_data_subscriber,
            pricing_updates=pricing_updates,
            session_registry=session_registry,
        )
        