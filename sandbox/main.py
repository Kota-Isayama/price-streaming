import asyncio
import datetime
from decimal import Decimal

from request_for_quote.application.pricing.runner import PricingWorker
from request_for_quote.domain.market.market import MarketDataValue, MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import JPY_OIS, SwapPricer
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive


async def main() -> None:
    market_state = MarketState()
    pricer = SwapPricer()

    worker = PricingWorker(
        market_state=market_state,
        pricer=pricer,
    )

    # Pricing開始前にMarket Stateが存在している想定
    await worker.market_updated(
        JPY_OIS,
        MarketDataValue(Decimal("0.0100")),
    )

    swap = InterestRateSwap(
        notional=Decimal("1000000000"),
        effective_date=datetime.date(2026, 9, 11),
        maturity_date=datetime.date(2036, 9, 11),
        fixed_leg=PayReceive.PAY,
        currency=Currency.JPY,
    )

    request = SwapPricingRequest(
        request_id="RFQ-123",
        revision=1,
        product=swap,
    )

    await worker.activate(
        request=request,
        dependencies={JPY_OIS},
    )

    await worker.market_updated(
        JPY_OIS,
        MarketDataValue(Decimal("0.0110")),
    )

    await worker.market_updated(
        JPY_OIS,
        MarketDataValue(Decimal("0.0130")),
    )


if __name__ == "__main__":
    asyncio.run(main())
