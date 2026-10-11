from dataclasses import dataclass
import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self

from request_for_quote.domain.market.identifier import CurveId
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.domain.shared.error import DomainError


class CurveInterpolation(StrEnum):
    LOG_LINEAR_DISCOUNT = "log_linear_discount"


@dataclass(frozen=True, slots=True)
class CurveNode:
    pillar_date: datetime.date
    discount_factor: Decimal

    def __post_init__(self) -> None:
        if self.discount_factor <= 0:
            raise InvalidCurveNode.discount_factor_is_not_positive(self.discount_factor)


class InvalidCurveNode(DomainError):
    @classmethod
    def discount_factor_is_not_positive(cls, discount_factor: Decimal) -> Self:
        msg = f"Discount factor must be positive, but {discount_factor}."
        return cls(msg)


@dataclass(frozen=True, slots=True)
class MarketCurve:
    curve_id: CurveId

    reference_date: datetime.date
    as_of: AwareDateTime

    nodes: tuple[CurveNode, ...]

    interpolation: CurveInterpolation

    def __post_init__(self) -> None:
        if not self.nodes:
            raise InvalidMarketCurve.nodes_are_empty()

        dates = tuple(node.pillar_date for node in self.nodes)
        if dates != tuple(sorted(dates)):
            raise InvalidMarketCurve.dates_are_not_sorted()
        if len(dates) != len(set(dates)):
            raise InvalidMarketCurve.dates_are_duplicated()

        first_node = self.nodes[0]

        if first_node.pillar_date != self.reference_date:
            raise InvalidMarketCurve.first_node_pillar_date_is_not_reference_date(pillar_date=first_node.pillar_date, reference_date=self.reference_date)
        if first_node.discount_factor != Decimal("1"):
            raise InvalidMarketCurve.reference_date_discount_factor_is_not_one(first_node.discount_factor)
        

class InvalidMarketCurve(DomainError):
    @classmethod
    def nodes_are_empty(cls) -> Self:
        msg = "MarketCurve must contain at least one node."
        return cls(msg)

    @classmethod
    def dates_are_not_sorted(cls) -> Self:
        msg = "Curve nodes must be ordered by pillar date."
        return cls(msg)

    @classmethod
    def dates_are_duplicated(cls) -> Self:
        msg = "MarketCurve must unique node dates."
        return cls(msg)

    @classmethod
    def first_node_pillar_date_is_not_reference_date(cls, pillar_date: datetime.date, reference_date: datetime.date) -> Self:
        msg = f"First node must be the reference date, but {pillar_date=} and {reference_date=}."
        return cls(msg)

    @classmethod
    def reference_date_discount_factor_is_not_one(cls, discount_factor: Decimal) -> Self:
        msg = f"Reference date discount factor must be 1, but {discount_factor}."
        return cls(msg)
    