import dataclasses
from typing import Self

from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import SwapPricingRequest


@dataclasses.dataclass(frozen=True)
class PricingSession:
    request: SwapPricingRequest
    dependencies: set[MarketDataId]

    is_pricing: bool = False
    reprice_requested: bool = False


    def with_new_request(
        self,
        new_request: SwapPricingRequest,
    ) -> Self:
        return dataclasses.replace(self, request=new_request)

    def with_new_dependencies(
        self,
        new_dependencies: set[MarketDataId],
    ) -> Self:
        return dataclasses.replace(self, dependencies=new_dependencies)
    
    