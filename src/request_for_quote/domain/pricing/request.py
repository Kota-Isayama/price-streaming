import dataclasses

from request_for_quote.domain.product.swap import InterestRateSwap


@dataclasses.dataclass(frozen=True)
class SwapPricingRequest:
    request_id: str
    revision: int
    product: InterestRateSwap


@dataclasses.dataclass(frozen=True)
class BondPricingRequest:
    request_id: str
    revision: int
    product: InterestRateSwap
    