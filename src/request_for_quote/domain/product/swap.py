import dataclasses
import datetime
from decimal import Decimal
import enum

from request_for_quote.domain.product.shared import Currency


class PayReceive(enum.StrEnum):
    PAY = "pay"
    RECEIVE = "receive"


@dataclasses.dataclass(frozen=True)
class InterestRateSwap:
    notional: Decimal

    effective_date: datetime.date
    maturity_date: datetime.date

    fixed_leg: PayReceive

    currency: Currency