from request_for_quote.application.pricing.session import PricingSession
from request_for_quote.domain.market.market import MarketDataId, MarketDataValue, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import SwapPrice, SwapPricer


class PricingUseCase:
    def __init__(
        self,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._market_state = market_state
        self._pricer = pricer

        self._sessions: dict[str, PricingSession] = {}

    async def activate(self, request: SwapPricingRequest, dependencies: set[MarketDataId]) -> None:
        session = PricingSession(
        request=request,
        dependencies=dependencies,
    )

        self._sessions[request.request_id] = session

        print(
            f"[ACTIVATE] rfq={request.request_id}"
        )

        if self._market_state.contains_all(
            dependencies
        ):
            await self._price(session)
        else:
            print(
                f"[WAITING MARKET] "
                f"rfq={request.request_id}"
            )

    async def on_market_updated(
        self,
        market_data_id: MarketDataId,
        value: MarketDataValue,
    ) -> None:
        self._market_state.update(
            market_data_id,
            value,
        )

        print(
            f"[MARKET] "
            f"{market_data_id.value}={value.value}"
        )

        for session in self._sessions.values():
            if market_data_id not in session.dependencies:
                continue

            await self._price(session)

    async def stop(
        self,
        request_id: str,
    ) -> None:
        self._sessions.pop(request_id, None)

        print(
            f"[STOP] request={request_id}"
        )

    async def _price(self, session: PricingSession) -> SwapPrice:
        snapshot = self._market_state.snapshot(session.dependencies)

        price = self._pricer.price(
            request=session.request,
            market=snapshot,
        )

        print(
            f"[PRICE] "
            f"rfq={session.request.request_id} "
            f"par_rate={price.par_rate}"
        )

        return price
    