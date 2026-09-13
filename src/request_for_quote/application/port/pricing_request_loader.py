import abc
import dataclasses

from request_for_quote.domain.market.market import MarketDataId
from request_for_quote.domain.pricing.request import SwapPricingRequest


@dataclasses.dataclass(frozen=True)
class LoadedPricingRequest:
    request: SwapPricingRequest
    dependencies: set[MarketDataId]


class IPricingRequestLoader(abc.ABC):
    async def load(
        self,
        request_id: str,
        revision: int,
    ) -> LoadedPricingRequest:
        raise NotImplementedError
    