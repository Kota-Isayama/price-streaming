from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable
import uuid

from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import PayReceive


class ChangeRfqUseCase:
    def __init__(
        self,
        uow_factory: Callable[[], IRfqUnitOfWork],
    ) -> None:
        self._uow_factory = uow_factory

    async def execute(
        self,
        rfq_id: str,
        notional: Decimal | None,
        effective_date: date | None,
        maturity_date: date | None,
        fixed_leg: PayReceive | None,
        currency: Currency | None,
    ) -> int:
        async with self._uow_factory() as uow:
            rfq = await uow.get_rfq_repository().get_by_id(rfq_id=rfq_id)

            if rfq is None:
                raise RuntimeError(
                    f"RFQ not found: {rfq_id}"
                )

            product = rfq.product
            new_product = product.with_change(
                notional=notional,
                effective_date=effective_date,
                maturity_date=maturity_date,
                fixed_leg=fixed_leg,
                currency=currency,
            )
            rfq.change_product(product=new_product)

            await uow.get_rfq_repository().save(rfq)

            pricing_request = SwapPricingRequest(
                request_id=rfq_id,
                revision=rfq.revision,
                product=new_product,
            )

            await uow.get_pricing_request_repository().save(pricing_request)

            await uow.get_outbox_repository().add(
                OutboxEvent(
                    event_id=str(uuid.uuid4()),
                    event_type="rfq_pricing_changed",
                    payload={
                        "rfq_id": rfq.rfq_id,
                        "revision": rfq.revision,
                    },
                    created_at=datetime.now(timezone.utc),
                )
            )

            await uow.commit()

        return rfq.revision
