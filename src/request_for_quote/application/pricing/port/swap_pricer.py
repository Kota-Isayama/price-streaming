import abc

from request_for_quote.application.pricing.model.pricing_dependencies import PricingDependencies
from request_for_quote.application.pricing.model.pricing_result import PricingResult
from request_for_quote.domain.product.interest_rate import InterestRateSwap


class ISwapPricer(abc.ABC):
    @abc.abstractmethod
    def price(
        self,
        *,
        swap: InterestRateSwap,
        dependencies: PricingDependencies,
    ) -> PricingResult:
        raise NotImplementedError
    