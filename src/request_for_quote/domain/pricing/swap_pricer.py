import dataclasses
from decimal import Decimal
import random

from request_for_quote.domain.market.market import MarketDataId, MarketSnapshot
from request_for_quote.domain.pricing.request import SwapPricingRequest

JPY_OIS = MarketDataId("JPY-OIS")


@dataclasses.dataclass(frozen=True)
class SwapPrice:
    par_rate: Decimal


class SwapPricer:
    def price(
        self,
        request: SwapPricingRequest,
        market: MarketSnapshot,
    ) -> SwapPrice:
        market_rate = market.get(JPY_OIS)

        # Pricing workerの動作確認用
        # 金融的に意味のあるPricingではない
        mock_spread = Decimal(random.random())

        return SwapPrice(
            par_rate=mock_spread
        )
    