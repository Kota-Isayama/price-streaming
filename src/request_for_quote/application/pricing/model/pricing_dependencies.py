from dataclasses import dataclass

from request_for_quote.domain.market.market_curve import MarketCurve
from request_for_quote.domain.product.interest_rate.rate_specification import FloatingRateIndex


class ProjectionCurveNotResolvedError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class ProjectionCurve:
    floating_rate_index: FloatingRateIndex
    curve: MarketCurve


@dataclass(frozen=True, slots=True)
class PricingDependencies:
    discount_curve: MarketCurve

    projection_curves: tuple[ProjectionCurve, ...]

    def projection_curve_for(
        self,
        floating_rate_index: FloatingRateIndex,
    ) -> MarketCurve:
        for projection_curve in self.projection_curves:
            if projection_curve.floating_rate_index == floating_rate_index:
                return projection_curve.curve

        raise ProjectionCurveNotResolvedError(
            "Projection curve was not resoleved "
            f"for index: {floating_rate_index.value}"
        )

    