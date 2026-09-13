# presentation/fastapi/dependencies.py

from typing import Annotated

from fastapi import Depends, Request

from request_for_quote.application.rfq.use_case.change_rfq import ChangeRfqUseCase
from request_for_quote.application.rfq.use_case.create_rfq import (
    CreateRfqUseCase,
)
from request_for_quote.application.rfq.use_case.stop_rfq_pricing import StopRfqPricingUseCase
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

    return StopRfqPricingUseCase(
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
