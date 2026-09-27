from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from request_for_quote.application.port.integration_event_publisher import (
    IIntegrationEventPublisher,
)
from request_for_quote.bootstrap.provider.database import (
    provide_session_maker,
)
from request_for_quote.bootstrap.provider.rfq_events import (
    provide_rfq_events_publisher,
)
from request_for_quote.bootstrap.settings.outbox_worker import (
    OutboxWorkerSettings,
)


@dataclass(frozen=True)
class OutboxWorkerContainer:
    session_maker: async_sessionmaker[AsyncSession]
    publisher: IIntegrationEventPublisher


@asynccontextmanager
async def bootstrap_outbox_worker(
    settings: OutboxWorkerSettings,
) -> AsyncIterator[OutboxWorkerContainer]:

    async with AsyncExitStack() as stack:
        session_maker = await stack.enter_async_context(
            provide_session_maker(
                settings.database,
            )
        )

        publisher = await stack.enter_async_context(
            provide_rfq_events_publisher(
                settings.rfq_events,
            )
        )

        yield OutboxWorkerContainer(
            session_maker=session_maker,
            publisher=publisher,
        )
        