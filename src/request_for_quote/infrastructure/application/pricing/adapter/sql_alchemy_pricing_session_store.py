from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.application.pricing.port.pricing_session_store import IPricingSessionStore
from request_for_quote.application.pricing.session import PricingSession
from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.infrastructure.postgres.models import PricingSessionOrm


class SqlAlchemyPricingSessionStore(IPricingSessionStore):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, pricing_session: PricingSession):
        await self._session.merge(
            PricingSessionOrm(
                request_id=pricing_session.request.request_id,
                request={
                    "request_id": pricing_session.request.request_id,
                    "revision": pricing_session.request.revision,
                    "product": {
                        "notional": str(pricing_session.request.product.notional),
                        "effective_date": pricing_session.request.product.effective_date.isoformat(),
                        "maturity_date": pricing_session.request.product.maturity_date.isoformat(),
                        "fixed_leg": pricing_session.request.product.fixed_leg,
                        "currency": pricing_session.request.product.currency,
                    },
                },
                dependencies=[dep.value for dep in pricing_session.dependencies],
                status=str(pricing_session.status),
            )
        )


    async def get_by_id(self, request_id: str) -> PricingSession | None:
        result = await self._session.get(PricingSessionOrm, request_id)

        if result is None:
            return None

        return PricingSession(
            request=SwapPricingRequest(
                request_id=result.request_id,
                revision=result.request["revision"],
                product=InterestRateSwap(
                    notional=result.request["product"]["notional"],
                    effective_date=date.fromisoformat(result.request["product"]["effective_date"]),
                    maturity_date=date.fromisoformat(result.request["product"]["maturity_date"]),
                    fixed_leg=PayReceive(result.request["product"]["fixed_leg"]),
                    currency=Currency(result.request["product"]["currency"]),
                ),
            ),
            dependencies={MarketDataId(value) for value in result.dependencies},
            status=result.status,
        )

    async def list_active(self) -> list[PricingSession]:
        stmt = select(PricingSessionOrm).where(PricingSessionOrm.status == "active")

        result = await self._session.execute(stmt)

        rows = result.scalars().all()

        return [
            PricingSession(
                request=SwapPricingRequest(
                    request_id=result.request_id,
                    revision=result.request["revision"],
                    product=InterestRateSwap(
                        notional=result.request["product"]["notional"],
                        effective_date=date.fromisoformat(result.request["product"]["effective_date"]),
                        maturity_date=date.fromisoformat(result.request["product"]["maturity_date"]),
                        fixed_leg=PayReceive(result.request["product"]["fixed_leg"]),
                        currency=Currency(result.request["product"]["currency"]),
                    ),
                ),
                dependencies={MarketDataId(value) for value in result.dependencies},
                status=result.status,
            ) for result in rows
        ]
    
    