from typing import Callable

from request_for_quote.application.port.pricing_request_loader import IPricingRequestLoader
from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher
from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork
from request_for_quote.domain.market.market import MarketDataId, MarketState
from request_for_quote.domain.pricing.request_repository import IPricingRequestRepository
from request_for_quote.domain.pricing.swap_pricer import SwapPricer


class ChangePricingUseCase:
    def __init__(
        self,
        session_uow_factory: Callable[[], IPricingSessionUnitOfWork],
        session_store: IPricingSessionRegistry,
        request_repository: IPricingRequestRepository,
        market_state: MarketState,
        pricer: SwapPricer,
        pricing_update_publisher: IPricingUpdatePublisher,
    ) -> None:
        self._session_uow_factory = session_uow_factory
        self._session_store = session_store
        self._request_repository = request_repository
        self._market_state = market_state
        self._pricer = pricer
        self._pricing_update_publisher = pricing_update_publisher

    async def execute(
        self,
        request_id: str,
        revision: int,
    ) -> None:
        session = self._session_store.get(request_id)

        if session is None:
            async with self._session_uow_factory() as uow:
                session = await uow.get_pricing_session_store().get_by_id(request_id)

            if session is None:
                raise RuntimeError(
                    "Pricing session not found for "
                    f"changed RFQ: {request_id}"
                )
            
            await uow.get_pricing_session_store().save(session)

        #
        # 重複/古いeventを防ぐ
        #
        if revision <= session.request.revision:
            return

        loaded = await self._request_repository.get_by_id_and_revision(
            request_id,
            revision=revision,
        )

        session = session.with_new_request(loaded)
        session = session.with_new_dependencies({MarketDataId("JPY-OIS")})
        async with self._session_uow_factory() as uow:
            await uow.get_pricing_session_store().save(session)
            await uow.commit()
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

        await self._pricing_update_publisher.publish(request_id=request_id, price=price)

        print(
            f"[PRICE] "
            f"request={request_id} "
            f"revision={revision} "
            f"par_rate={price.par_rate}"
        )