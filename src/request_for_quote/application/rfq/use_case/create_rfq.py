from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable
import uuid

from request_for_quote.application.domain_event_dispatcher import DomainEventDispatcher
from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote
from request_for_quote.domain.shared.aware_datetime import AwareDateTime


class CreateRfqUseCase:
    def __init__(
        self,
        uow_factory: Callable[[], IRfqUnitOfWork],
        domain_event_dispatcher: DomainEventDispatcher,
    ) -> None:
        self._uow_factory = uow_factory
        self._domain_event_dispatcher = domain_event_dispatcher

    async def execute(
        self,
        *,
        notional: Decimal,
        effective_date: date,
        maturity_date: date,
        fixed_leg: str,
        currency: Currency,
        assigned_trader: str,
        registered_by: str,
    ) -> str:
        rfq_id = str(uuid.uuid4())
        now = AwareDateTime.now()

        swap = InterestRateSwap(
            notional=notional,
            effective_date=effective_date,
            maturity_date=maturity_date,
            fixed_leg=PayReceive(
                fixed_leg
            ),
            currency=currency,
        )

        rfq = RequestForQuote.register(
            rfq_id=rfq_id,
            product=swap,
            assigned_trader=assigned_trader,
            registered_by=registered_by,
            occurred_at=now,
        )

        async with self._uow_factory() as uow:
            await uow.get_rfq_repository().save(rfq)

            for domain_event in rfq.pull_domain_events():
                await self._domain_event_dispatcher.dispatch(
                    domain_event,
                    uow=uow,
                )

            await uow.commit()

        return rfq.rfq_id
    