from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from request_for_quote.application.port.notification_publisher import INotificationPublisher
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber
from request_for_quote.bootstrap.provider.database import provide_session_maker
from request_for_quote.bootstrap.provider.realtime_updates import provide_notification_updates_publisher
from request_for_quote.bootstrap.provider.rfq_events import provide_rfq_events_subscriber
from request_for_quote.bootstrap.settings.notification_worker import NotificationWorkerSettings


@dataclass(frozen=True)
class NotificationWorkerContainer:
    session_maker: async_sessionmaker[AsyncSession]

    rfq_events: IRfqDomainEventsSubscriber

    notification_updates: INotificationPublisher


@asynccontextmanager
async def bootstrap_notification_worker(
    settings: NotificationWorkerSettings,
) -> AsyncGenerator[NotificationWorkerContainer]:

    async with AsyncExitStack() as stack:
        session_maker = await stack.enter_async_context(
            provide_session_maker(
                settings.database,
            )
        )

        rfq_events = await stack.enter_async_context(
            provide_rfq_events_subscriber(
                settings.rfq_events,
            )
        )

        notification_updates = await stack.enter_async_context(
            provide_notification_updates_publisher(
                settings.notification_updates,
            )
        )

        yield NotificationWorkerContainer(
            session_maker=session_maker,
            rfq_events=rfq_events,
            notification_updates=notification_updates,
        )
        