from request_for_quote.domain.market.market import MarketDataId, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import SwapPrice, SwapPricer


class PricingSession:
    def __init__(
        self,
        request: SwapPricingRequest,
        dependencies: set[MarketDataId],
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._request = request
        self._dependencies = dependencies
        self._market_state = market_state
        self._pricer = pricer

    async def reprice(self) -> SwapPrice:
        market = self._market_state.snapshot(self._dependencies)

        price = self._pricer.price(
            request=self._request,
            market=market,
        )

        print(
            f"[PRICE] "
            f"rfq={self._request.request_id} "
            f"par_rate={price.par_rate}"
        )

        return price


    # Getter
    def dependencies(self) -> set[MarketDataId]:
        return self._dependencies
    