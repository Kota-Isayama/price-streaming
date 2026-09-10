from request_for_quote.application.port.pricing_session_store import IPricingSessionStore
from request_for_quote.domain.market.market import MarketState
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class ChangePricingUseCase:
    def __init__(
        self,
        session_store: IPricingSessionStore,
        request_loader: PricingRequestLoader,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._session_store = session_store
        self._request_loader = request_loader
        self._market_state = market_state
        self._pricer = pricer

    async def execute(
        self,
        request_id: str,
        revision: int,
    ) -> None:
        session = self._session_store.get(request_id)

        if session is None:
            # 今は簡略化。
            # Rebalance/recoveryを実装すると
            # ここは改めて考える。
            return

        #
        # 重複/古いeventを防ぐ
        #
        if revision <= session.request.revision:
            return

        loaded = await self._request_loader.load(
            request_id,
            revision=revision,
        )

        session.request = loaded.request
        session.dependencies = loaded.dependencies

        self._session_store.save(session)

        if not self._market_state.contains_all(
            session.dependencies
        ):
            return

        snapshot = self._market_state.snapshot(
            session.dependencies
        )

        price = self._pricer.price(
            request=session.request,
            market=snapshot,
        )

        print(
            f"[PRICE] "
            f"request={request_id} "
            f"revision={revision} "
            f"par_rate={price.par_rate}"
        )