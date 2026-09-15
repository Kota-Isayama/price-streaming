from typing import Callable

from request_for_quote.application.port.pricing_request_loader import IPricingRequestLoader
from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.pricing.port.pricing_session_store import IPricingSessionStore
from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork
from request_for_quote.application.pricing.session import PricingSession
from request_for_quote.domain.market.market import MarketDataId, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.request_repository import IPricingRequestRepository
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class ActivatePricingSessionUseCase:
    def __init__(
        self,
        pricing_session_uow_factory: Callable[[], IPricingSessionUnitOfWork],
        session_registry: IPricingSessionRegistry,
        request_repository: IPricingRequestRepository,
        market_state: MarketState,
        pricer: SwapPricer,
    ) -> None:
        self._pricing_session_uow_factory = pricing_session_uow_factory
        self._session_registry = session_registry
        self._request_repository = request_repository
        self._market_state = market_state
        self._pricer = pricer

    async def execute(
        self,
        request_id: str,
        revision: int,
    ) -> None:
        loaded = await self._request_repository.get_by_id_and_revision(request_id=request_id, revision=revision)

        if loaded is None:
            print(
                f"[WARNING] "
                f"pricing_request={request_id} was not found."
            )
            return

        request = loaded
        dependencies = {MarketDataId("JPY-OIS")}  # TODO: Dependenciesをちゃんと管理する。

        session = PricingSession(
            request=request,
            dependencies=dependencies,
            status="active",
        )

        async with self._pricing_session_uow_factory() as uow:
            await uow.get_pricing_session_store().save(session)
            await uow.commit()
            
        self._session_registry.save(session)

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