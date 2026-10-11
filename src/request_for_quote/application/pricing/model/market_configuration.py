from dataclasses import dataclass
from typing import Self

from request_for_quote.application.shared.error import ApplicationError
from request_for_quote.domain.market.identifier import CurveId
from request_for_quote.domain.product.interest_rate.rate_specification import FloatingRateIndex


class ProjectionCurveNotConfiguredError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class ProjectionCurveConfiguration:
    floating_rate_index: FloatingRateIndex
    curve_id: CurveId


@dataclass(frozen=True, slots=True)
class MarketConfiguration:
    discount_curve_id: CurveId

    projection_curves: tuple[ProjectionCurveConfiguration, ...]

    def __post_init__(self) -> None:
        indices = tuple(
            configuration.floating_rate_index
            for configuration in self.projection_curves
        )

        if len(indices) != len(set(indices)):
            raise InvalidMarketConfiguration.rate_indices_are_duplicated()

    def projection_curve_id_for(self, floating_rate_index: FloatingRateIndex) -> CurveId:
        for configuration in self.projection_curves:
            if configuration.floating_rate_index == floating_rate_index:
                return configuration.curve_id

        raise ProjectionCurveNotConfiguredError(
            "Projection curve is not configured "
            f"for index: {floating_rate_index.value}"
        )
    

class InvalidMarketConfiguration(ApplicationError):
    @classmethod
    def rate_indices_are_duplicated(cls) -> Self:
        msg = f"Floating Rate Indices must be unique."
        return cls(msg)
    