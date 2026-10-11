from dataclasses import dataclass
from decimal import Decimal
from typing import TypeAlias


@dataclass(frozen=True)
class FloatingRateIndex:
    value: str


@dataclass(frozen=True)
class FixedRateSpecification:
    rate: Decimal


@dataclass(frozen=True)
class FloatingRateSpecification:
    index: FloatingRateIndex
    spread: Decimal = Decimal("0")


RateSpecification: TypeAlias = FixedRateSpecification | FloatingRateSpecification
