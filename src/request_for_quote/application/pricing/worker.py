from request_for_quote.application.pricing.session import PricingSession
from request_for_quote.domain.market.market import MarketDataId, MarketDataValue, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class PricingWorker:
    def __init__(
        self,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._market_state = market_state
        self._pricer = pricer

        self._sessions: dict[str, PricingSession] = {}

    async def activate(
        self,
        request: SwapPricingRequest,
        dependencies: set[MarketDataId],
    ) -> None:
        session = PricingSession(
            request=request,
            dependencies=dependencies,
            market_state=self._market_state,
            pricer=self._pricer,
        )

        print(
            f"[ACTIVATE] rfq={request.request_id}"
        )

        await session.reprice()

    async def market_updated(
        self,
        market_data_id: MarketDataId,
        value: MarketDataValue,
    ) -> None:
        self._market_state.update(
            market_data_id=market_data_id,
            value=value,
        )

        print(
            f"[MARKET] "
            f"{market_data_id.value}={value.value}"
        )

        for session in self._sessions.values():
            if market_data_id in session._dependencies:
                await session.reprice()  # market_stateがsessionでも参照されていることが暗に仮定されており、よくない。repriceにmarket_stateを与えるようにした方がよい？

    async def stop(
        self,
        request_id: str,
    ) -> None:
        self._sessions.pop(request_id, None)

        print(
            f"[STOP] rfq={request_id}"
        )
        