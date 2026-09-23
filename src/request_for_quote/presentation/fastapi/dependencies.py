# presentation/fastapi/dependencies.py

import re
from typing import Annotated, AsyncIterator

from fastapi import Depends, Request
import redis.asyncio as redis

from request_for_quote.application.domain_event_dispatcher import DomainEventDispatcher
from request_for_quote.application.port.notification_subscriber import INotificationSubscriber
from request_for_quote.application.pricing.use_case.pricing_subscription_service import PricingSubscriptionService
from request_for_quote.application.rfq.event_handler import RfqRegisteredHandler
from request_for_quote.application.rfq.use_case.change_rfq import ChangeRfqUseCase
from request_for_quote.application.rfq.use_case.create_rfq import (
    CreateRfqUseCase,
)
from request_for_quote.application.rfq.use_case.get_rfq import GetRfqUseCase
from request_for_quote.application.rfq.use_case.list_rfqs import ListRfqsUseCase
from request_for_quote.application.rfq.use_case.stop_rfq_pricing import StopRfqPricingUseCase
from request_for_quote.domain.request_for_quote.events import RfqRegistredDomainEvent
from request_for_quote.infrastructure.application.adapter.notification_subscriber.redis.redis_notification_subscriber import RedisNotificationSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_udpate_subscriber.redis.redis_pricing_update_subscriber import RedisPricingUpdateSubscriber
from request_for_quote.infrastructure.postgres.base import (
    AsyncSessionMaker,
)
from request_for_quote.infrastructure.application.rfq.unit_of_work.sql_alchemy_unit_of_work import (
    SqlAlchemyUnitOfWork,
)
from request_for_quote.presentation.fastapi.container import (
    ApiContainer,
)


def get_container(
    request: Request,
) -> ApiContainer:
    return request.app.state.container


domain_event_dispacher = DomainEventDispatcher(
    handlers={
        RfqRegistredDomainEvent: [
            RfqRegisteredHandler(),
        ],
    },
)


ContainerDep = Annotated[
    ApiContainer,
    Depends(get_container),
]


def get_session_maker(
    container: ContainerDep,
) -> AsyncSessionMaker:
    return container.session_maker


SessionMakerDep = Annotated[
    AsyncSessionMaker,
    Depends(get_session_maker),
]

def get_create_rfq_use_case(
    session_maker: SessionMakerDep,
) -> CreateRfqUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=session_maker,
        )

    return CreateRfqUseCase(
        uow_factory=uow_factory,
        domain_event_dispatcher=domain_event_dispacher,
    )


def get_change_rfq_use_case(
    session_maker: SessionMakerDep,
) -> ChangeRfqUseCase:
    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=session_maker,
        )

    return ChangeRfqUseCase(
        uow_factory=uow_factory,
    )


def get_stop_rfq_pricing_use_case(
    session_maker: SessionMakerDep,
) -> ChangeRfqUseCase:
    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=session_maker,
        )

    return ChangeRfqUseCase(
        uow_factory=uow_factory,
    )


def get_list_rfqs_use_case(
    session_maker: SessionMakerDep,
) -> ListRfqsUseCase:
    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=session_maker,
        )

    return ListRfqsUseCase(uow_factory=uow_factory)


def get_get_rfq_use_case(
    session_maker: SessionMakerDep,
) -> GetRfqUseCase:
    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=session_maker,
        )

    return GetRfqUseCase(uow_factory=uow_factory)


CreateRfqUseCaseDep = Annotated[
    CreateRfqUseCase,
    Depends(get_create_rfq_use_case),
]


ChangeRfqUseCaseDep = Annotated[
    ChangeRfqUseCase,
    Depends(get_change_rfq_use_case),
]

StopRfqPricingUseCaseDep = Annotated[
    StopRfqPricingUseCase,
    Depends(get_stop_rfq_pricing_use_case),
]

ListRfqsUseCaseDep = Annotated[
    ListRfqsUseCase,
    Depends(get_list_rfqs_use_case),
]


GetRfqUseCaseDep = Annotated[
    GetRfqUseCase,
    Depends(get_get_rfq_use_case),
]

def get_redis_client() -> redis.Redis:
    print("got")
    return redis.Redis.from_url("redis://localhost:6379")


async def get_pricing_subscription_service(
    redis_client: redis.Redis = Depends(get_redis_client),
) -> AsyncIterator[PricingSubscriptionService]:
    print("1. dependency entered")

    pubsub = redis_client.pubsub()

    subscriber = RedisPricingUpdateSubscriber(
        pubsub=pubsub,
    )

    service = PricingSubscriptionService(
        subscriber=subscriber,
    )

    print("2. dependency yielding")

    try:
        yield service
    finally:
        print("5. dependency closing")
        await service.close()


PricingSubscriptionServiceDep = Annotated[
    PricingSubscriptionService,
    Depends(get_pricing_subscription_service),
]


redis_client = redis.Redis.from_url(
    "redis://localhost:6379",
    decode_responses=True,
)


notification_subscriber = (
    RedisNotificationSubscriber(
        redis_client
    )
)


async def get_notification_subscriber(
    redis_client: redis.Redis = Depends(get_redis_client),
) -> INotificationSubscriber:
    print("1")
    return RedisNotificationSubscriber(
        redis_client=redis_client,
    )
    print("2")


NotificationSubscriberDep = Annotated[
    INotificationSubscriber,
    Depends(get_notification_subscriber)
]
