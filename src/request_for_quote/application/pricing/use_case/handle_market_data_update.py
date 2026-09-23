from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher
from request_for_quote.domain.market.market import MarketDataId, MarketDataValue, MarketState
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class HandleMarketDataUpdateUseCase:
    def __init__(
        self,
        session_store: IPricingSessionRegistry,
        market_state: MarketState,
        pricer: SwapPricer,
        pricing_update_publisher: IPricingUpdatePublisher,
    ) -> None:
        self._session_store = session_store
        self._market_state = market_state
        self._pricer = pricer  # よくよく考えたらこのuse caseがpricerを注入されるのはアリ？ Pricerはdomain層では？
        self._pricing_update_publisher = pricing_update_publisher

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

            await self._pricing_update_publisher.publish(request_id=session.request.request_id, price=price)

            print(
                f"[PRICE] "
                f"request={session.request.request_id} "
                f"par_rate={price.par_rate}"
            )
