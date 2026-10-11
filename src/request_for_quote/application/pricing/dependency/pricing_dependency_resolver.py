from request_for_quote.application.pricing.model import (
    MarketConfiguration,
    PricingDependencies,
    ProjectionCurve,
)
from request_for_quote.application.pricing.port.curve_provider import (
    ICurveProvider,
)
from request_for_quote.domain.product.interest_rate import (
    FloatingRateIndex,
    FloatingRateSpecification,
    InterestRateSwap,
)


class PricingDependencyResolver:
    def __init__(
        self,
        *,
        curve_provider: ICurveProvider,
    ) -> None:
        self._curve_provider = curve_provider

    def resolve(
        self,
        *,
        product: InterestRateSwap,
        market_data_configuration: (
            MarketConfiguration
        ),
    ) -> PricingDependencies:
        discount_curve = (
            self._curve_provider.get(
                market_data_configuration
                .discount_curve_id
            )
        )

        floating_rate_indices = (
            self._collect_floating_rate_indices(
                product
            )
        )

        projection_curves = tuple(
            ProjectionCurve(
                floating_rate_index=index,
                curve=self._curve_provider.get(
                    market_data_configuration
                    .projection_curve_id_for(
                        index
                    )
                ),
            )
            for index
            in floating_rate_indices
        )

        return PricingDependencies(
            discount_curve=discount_curve,
            projection_curves=(
                projection_curves
            ),
        )

    @staticmethod
    def _collect_floating_rate_indices(
        product: InterestRateSwap,
    ) -> tuple[
        FloatingRateIndex,
        ...,
    ]:
        indices: list[
            FloatingRateIndex
        ] = []

        for payout in product.payouts:
            rate_specification = (
                payout.rate_specification
            )

            if not isinstance(
                rate_specification,
                FloatingRateSpecification,
            ):
                continue

            index = (
                rate_specification.index
            )

            if index not in indices:
                indices.append(index)

        return tuple(indices)
    