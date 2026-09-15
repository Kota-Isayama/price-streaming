import dataclasses
import enum
from typing import Self

from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import SwapPricingRequest


class PricingSessionStatus(enum.Enum):
    ACTIVE = "active"
    DEACTIVE = "deactive"


@dataclasses.dataclass(frozen=True)
class PricingSession:
    request: SwapPricingRequest
    dependencies: set[MarketDataId]

    status: PricingSessionStatus

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

    def deactivated(self) -> Self:
        return dataclasses.replace(self, status=PricingSessionStatus.DEACTIVE)
    
    