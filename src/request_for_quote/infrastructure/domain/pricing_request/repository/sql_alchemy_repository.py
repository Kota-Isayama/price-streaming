from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.infrastructure.postgres.models import PricingRequestOrm


class SqlAlchemyPricingRequestRepository:
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self._session = session

    async def save(
        self,
        revision: SwapPricingRequest,
    ) -> None:
        product = revision.product

        self._session.add(
            PricingRequestOrm(
                request_id=revision.request_id,
                revision=revision.revision,
                product_type="JPY_IRS",
                payload={
                    "notional": str(product.notional),
                    "effective_date": (
                        product.effective_date.isoformat()
                    ),
                    "maturity_date": (
                        product.maturity_date.isoformat()
                    ),
                    "fixed_leg": product.fixed_leg.value,
                    "currency": product.currency.value,
                },
            )
        )

    async def get_by_id_and_revision(
        self,
        request_id: str,
        revision: int,
    ) -> SwapPricingRequest:
        result = await self._session.execute(
            select(PricingRequestOrm)
            .where(
                PricingRequestOrm.request_id==request_id,
                PricingRequestOrm.revision == revision,
            )
        )

        orm = result.scalar_one()

        return SwapPricingRequest(
            request_id=orm.request_id,
            revision=orm.revision,
            product=InterestRateSwap(
                notional=Decimal(orm.payload["notional"]),
                effective_date=date.fromisoformat(orm.payload["effective_date"]),
                maturity_date=date.fromisoformat(orm.payload["maturity_date"]),
                fixed_leg=PayReceive(orm.payload["fixed_leg"]),
                currency=Currency(orm.payload["currency"]),
            ),
        )
    