# bootstrap/processes/api.py

from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.port.notification_subscriber import (
    INotificationSubscriber,
)
from request_for_quote.application.port.pricing_update_subscriber import (
    IPricingUpdateSubscriber,
)
from request_for_quote.bootstrap.provider.database import (
    provide_session_maker,
)
from request_for_quote.bootstrap.provider.realtime_updates import (
    provide_notification_updates_subscriber_factory,
    provide_pricing_updates_subscriber_factory,
)
from request_for_quote.bootstrap.settings.api import ApiSettings


NotificationSubscriberFactory = Callable[
    [],
    INotificationSubscriber,
]

PricingUpdateSubscriberFactory = Callable[
    [],
    IPricingUpdateSubscriber,
]


@dataclass(frozen=True)
class ApiContainer:
    session_maker: async_sessionmaker[AsyncSession]

    notification_updates_factory: NotificationSubscriberFactory
    pricing_updates_factory: PricingUpdateSubscriberFactory


@asynccontextmanager
async def bootstrap_api(
    settings: ApiSettings,
) -> AsyncGenerator[ApiContainer]:
    async with AsyncExitStack() as stack:
        session_maker = await stack.enter_async_context(
            provide_session_maker(
                settings.database,
            )
        )

        notification_updates_factory = (
            await stack.enter_async_context(
                provide_notification_updates_subscriber_factory(
                    settings.notification_updates,
                )
            )
        )

        pricing_updates_factory = (
            await stack.enter_async_context(
                provide_pricing_updates_subscriber_factory(
                    settings.pricing_updates,
                )
            )
        )

        yield ApiContainer(
            session_maker=session_maker,
            notification_updates_factory=(
                notification_updates_factory
            ),
            pricing_updates_factory=(
                pricing_updates_factory
            ),
        )