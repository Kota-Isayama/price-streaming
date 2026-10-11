from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class PricingResult:
    npv: Decimal
    fair_rate: Decimal
