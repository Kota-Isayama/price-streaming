from typing import Annotated, AsyncIterator

from fastapi import Depends, Request
from fastapi.requests import HTTPConnection

from request_for_quote.application.domain_event_dispatcher import (
    DomainEventDispatcher,
)
from request_for_quote.application.port.notification_subscriber import (
    INotificationSubscriber,
)
from request_for_quote.application.pricing.use_case.pricing_subscription_service import (
    PricingSubscriptionService,
)
from request_for_quote.application.rfq.event_handler import (
    RfqRegisteredHandler,
)
from request_for_quote.application.rfq.use_case.change_rfq import (
    ChangeRfqUseCase,
)
from request_for_quote.application.rfq.use_case.create_rfq import (
    CreateRfqUseCase,
)
from request_for_quote.application.rfq.use_case.get_rfq import (
    GetRfqUseCase,
)
from request_for_quote.application.rfq.use_case.list_rfqs import (
    ListRfqsUseCase,
)
from request_for_quote.application.rfq.use_case.stop_rfq_pricing import (
    StopRfqPricingUseCase,
)
from request_for_quote.bootstrap.processes.api import ApiContainer
from request_for_quote.domain.request_for_quote.events import (
    RfqRegistredDomainEvent,
)
from request_for_quote.infrastructure.application.rfq.unit_of_work.sql_alchemy_unit_of_work import (
    SqlAlchemyUnitOfWork,
)



# --------------------------------------------------
# Application wiring
# --------------------------------------------------

domain_event_dispatcher = DomainEventDispatcher(
    handlers={
        RfqRegistredDomainEvent: [
            RfqRegisteredHandler(),
        ],
    },
)


# --------------------------------------------------
# Container
# --------------------------------------------------

def get_container(
    connection: HTTPConnection,
) -> ApiContainer:
    return connection.app.state.container


ContainerDep = Annotated[
    ApiContainer,
    Depends(get_container),
]


# --------------------------------------------------
# RFQ UseCases
# --------------------------------------------------

def get_create_rfq_use_case(
    container: ContainerDep,
) -> CreateRfqUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=container.session_maker,
        )

    return CreateRfqUseCase(
        uow_factory=uow_factory,
        domain_event_dispatcher=domain_event_dispatcher,
    )


def get_change_rfq_use_case(
    container: ContainerDep,
) -> ChangeRfqUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=container.session_maker,
        )

    return ChangeRfqUseCase(
        uow_factory=uow_factory,
    )


def get_stop_rfq_pricing_use_case(
    container: ContainerDep,
) -> StopRfqPricingUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=container.session_maker,
        )

    return StopRfqPricingUseCase(
        uow_factory=uow_factory,
    )


def get_list_rfqs_use_case(
    container: ContainerDep,
) -> ListRfqsUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=container.session_maker,
        )

    return ListRfqsUseCase(
        uow_factory=uow_factory,
    )


def get_get_rfq_use_case(
    container: ContainerDep,
) -> GetRfqUseCase:

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(
            session_maker=container.session_maker,
        )

    return GetRfqUseCase(
        uow_factory=uow_factory,
    )


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


# --------------------------------------------------
# Pricing WS
# --------------------------------------------------

async def get_pricing_subscription_service(
    container: ContainerDep,
) -> AsyncIterator[PricingSubscriptionService]:

    subscriber = container.pricing_updates_factory()

    try:
        yield PricingSubscriptionService(subscriber=subscriber)
    finally:
        await subscriber.close()


PricingSubscriptionServiceDep = Annotated[
    PricingSubscriptionService,
    Depends(get_pricing_subscription_service),
]


# --------------------------------------------------
# Notification WS
# --------------------------------------------------

def get_notification_subscriber(
    container: ContainerDep,
) -> INotificationSubscriber:
    return container.notification_updates_factory()


NotificationSubscriberDep = Annotated[
    INotificationSubscriber,
    Depends(get_notification_subscriber),
]
