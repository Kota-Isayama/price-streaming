from request_for_quote.application.port.pricing_session_store import IPricingSessionStore
from request_for_quote.application.pricing.session import PricingSession
from request_for_quote.domain.market.market import MarketDataId, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class ActivatePricingSessionUseCase:
    def __init__(
        self,
        session_store: IPricingSessionStore,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._session_store = session_store
        self._market_state = market_state
        self._pricer = pricer

    async def execute(
        self,
        request: SwapPricingRequest,
        dependencies: set[MarketDataId],
    ) -> None:
        session = PricingSession(
            request=request,
            dependencies=dependencies,
        )

        self._session_store.save(session)

        print(
            f"[ACTIVATE] ",
            f"pricing_request={request.request_id}"
        )

        if not self._market_state.contains_all(
            dependencies
        ):
            print(
                f"[WAITING MARKET] "
                f"rfq={request.request_id}"
            )
            return

        snapshot = self._market_state.snapshot(
            dependencies
        )

        price = self._pricer.price(  # そもそもactivate時にpricingが勝手にくっついてくるのは副作用がすぎないか？
            request=request,
            market=snapshot,
        )

        print(
            f"[PRICE] "
            f"rfq={request.request_id} "
            f"par_rate={price.par_rate}"
        )