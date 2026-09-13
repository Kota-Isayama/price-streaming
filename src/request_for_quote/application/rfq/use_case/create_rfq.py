from datetime import date, datetime, timezone
from decimal import Decimal
import uuid

from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote


class CreateRfqUseCase:
    def __init__(
        self,
        uow_factory,
    ) -> None:
        self._uow_factory = uow_factory

    async def execute(
        self,
        *,
        notional: Decimal,
        effective_date: date,
        maturity_date: date,
        fixed_leg: str,
        currency: Currency,
    ) -> str:
        rfq_id = str(uuid.uuid4())

        swap = InterestRateSwap(
            notional=notional,
            effective_date=effective_date,
            maturity_date=maturity_date,
            fixed_leg=PayReceive(
                fixed_leg
            ),
            currency=currency,
        )

        rfq = RequestForQuote(
            rfq_id=rfq_id,
            revision=1,
            product=swap,
        )

        pricing_request = SwapPricingRequest(
            request_id=rfq.rfq_id,
            revision=rfq.revision,
            product=swap,
        )


        event = OutboxEvent(
            event_id=str(uuid.uuid4()),
            event_type="rfq_pricing_activated",
            payload={
                "rfq_id": rfq.rfq_id,
                "revision": rfq.revision,
            },
            created_at=datetime.now(timezone.utc),
        )

        async with self._uow_factory() as uow:
            await uow.rfqs.save(rfq)

            await uow.pricing_requests.save(
                pricing_request
            )

            await uow.outbox.add(event)

            await uow.commit()

        return rfq.rfq_id
    