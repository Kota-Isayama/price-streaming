from request_for_quote.application.port.pricing_session_store import IPricingSessionStore
from request_for_quote.domain.market.market import MarketDataId, MarketDataValue, MarketState
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class HandleMarketDataUpdateUseCase:
    def __init__(
        self,
        session_store: IPricingSessionStore,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._session_store = session_store
        self._market_state = market_state
        self._pricer = pricer  # よくよく考えたらこのuse caseがpricerを注入されるのはアリ？ Pricerはdomain層では？

    async def execute(
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

        sessions = self._session_store.list_all()

        for session in sessions:
            if market_data_id not in session.dependencies:
                continue

            if not self._market_state.contains_all(session.dependencies):
                continue

            snapshot = self._market_state.snapshot(session.dependencies)

            price = self._pricer.price(
                request=session.request,
                market=snapshot,
            )

            print(
                f"[PRICE] "
                f"request={session.request.request_id} "
                f"par_rate={price.par_rate}"
            )
            