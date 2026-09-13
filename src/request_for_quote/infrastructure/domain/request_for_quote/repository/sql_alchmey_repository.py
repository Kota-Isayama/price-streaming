from datetime import date
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.domain.product import Product
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.domain.request_for_quote.repository import IRfqRepository
from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote
from request_for_quote.infrastructure.postgres.models import RfqOrm


class SqlAlchemyRfqRepository(IRfqRepository):
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self._session = session

    async def save(
        self,
        rfq: RequestForQuote,
    ) -> None:
        orm = RfqOrm(
            rfq_id=rfq.rfq_id,
            revision=rfq.revision,
            product_type="JPY_IRS",
            payload={
                "notional": str(rfq.product.notional),
                "effective_date": rfq.product.effective_date.isoformat(),
                "maturity_date": rfq.product.maturity_date.isoformat(),
                "fixed_leg": rfq.product.fixed_leg.value,
                "currency": rfq.product.currency.value,
            },
        )

        await self._session.merge(orm)

    async def get_by_id(
        self,
        rfq_id: str,
    ) -> RequestForQuote | None:
        orm = await self._session.get(
            RfqOrm,
            rfq_id,
        )

        if orm is None:
            return None

        payload = orm.payload

        product = InterestRateSwap(
            notional=Decimal(payload["notional"]),
            effective_date=date.fromisoformat(
                payload["effective_date"]
            ),
            maturity_date=date.fromisoformat(
                payload["maturity_date"]
            ),
            fixed_leg=PayReceive(payload["fixed_leg"]),
            currency=Currency(payload["currency"])
        )

        return RequestForQuote(
            rfq_id=rfq_id,
            revision=orm.revision,
            product=product,
        )
    