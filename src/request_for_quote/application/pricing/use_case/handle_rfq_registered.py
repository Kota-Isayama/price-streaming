from decimal import Decimal

from request_for_quote.application.integration.events.rfq import (
    RfqRegisteredIntegrationEvent,
)
from request_for_quote.application.port.pricing_session_registry import (
    IPricingSessionRegistry,
)
from request_for_quote.application.pricing.session import (
    PricingSession,
    PricingSessionStatus,
)
from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import (
    SwapPricingRequest,
)
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import (
    InterestRateSwap,
    PayReceive,
)


class HandleRfqRegisteredForPricingUseCase:
    def __init__(
        self,
        *,
        pricing_session_uow_factory,
        session_registry: IPricingSessionRegistry,
        market_state,
        pricer,
        pricing_update_publisher,
    ) -> None:
        self._pricing_session_uow_factory = (
            pricing_session_uow_factory
        )
        self._session_registry = session_registry
        self._market_state = market_state
        self._pricer = pricer
        self._pricing_update_publisher = (
            pricing_update_publisher
        )

    async def execute(
        self,
        event: RfqRegisteredIntegrationEvent,
    ) -> None:
        product = event.product

        swap = InterestRateSwap(
            notional=Decimal(product.notional),
            effective_date=product.effective_date,
            maturity_date=product.maturity_date,
            fixed_leg=PayReceive(product.fixed_leg),
            currency=Currency(product.currency),
        )

        request = SwapPricingRequest(
            request_id=event.rfq_id,
            revision=event.revision,
            product=swap,
        )

        # TODO:
        # 商品情報などから依存MarketDataを導出する仕組みにする
        dependencies = {
            MarketDataId("JPY-OIS"),
        }

        session = PricingSession(
            request=request,
            dependencies=dependencies,
            status=PricingSessionStatus.ACTIVE,
        )

        # Durable stateを先に保存
        async with self._pricing_session_uow_factory() as uow:
            await uow.get_pricing_session_store().save(
                session
            )

            await uow.commit()

        # process-local stateへ反映
        self._session_registry.save(session)

        print(
            "[ACTIVATE] "
            f"pricing_request={request.request_id}"
        )

        # Market Dataがまだ揃っていなければ、
        # 次回Market Updateでpricingされる
        if not self._market_state.contains_all(
            dependencies
        ):
            print(
                "[WAITING MARKET] "
                f"rfq={request.request_id}"
            )
            return

        snapshot = self._market_state.snapshot(
            dependencies
        )

        price = self._pricer.price(
            request=request,
            market=snapshot,
        )

        await self._pricing_update_publisher.publish(
            request_id=request.request_id,
            price=price,
        )

        print(
            "[PRICE] "
            f"rfq={request.request_id} "
            f"par_rate={price.par_rate}"
        )