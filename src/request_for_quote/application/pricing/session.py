import dataclasses

from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import SwapPricingRequest


@dataclasses.dataclass(frozen=True)
class PricingSession:
    request: SwapPricingRequest
    dependencies: set[MarketDataId]

    is_pricing: bool = False
    reprice_requested: bool = False
    