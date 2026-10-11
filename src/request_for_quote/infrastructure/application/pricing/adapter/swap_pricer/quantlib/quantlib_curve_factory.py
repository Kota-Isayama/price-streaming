import QuantLib as ql

from request_for_quote.domain.market.market_curve import CurveInterpolation, MarketCurve
from request_for_quote.infrastructure.quantlib.util import to_quantlib_date


class QuantLibCurveFactory:
    def build(
        self,
        curve: MarketCurve,
    ) -> ql.DiscountCurve:
        if curve.interpolation != CurveInterpolation.LOG_LINEAR_DISCOUNT:
            raise ValueError(
                "Unsupported curve interpolation: "
                f"{curve.interpolation.value}"
            )

        dates = [
            to_quantlib_date(node.pillar_date) for node in curve.nodes
        ]

        discount_factors = [
            float(node.discount_factor) for node in curve.nodes
        ]

        return ql.DiscountCurve(
            dates,
            discount_factors,
            ql.Actual365Fixed(),  # TODO: Get daycount from argument.
        )