import dataclasses
import datetime
from decimal import Decimal
import enum
from typing import Self

from request_for_quote.domain.product.shared import Currency


class PayReceive(enum.StrEnum):
    PAY = "pay"
    RECEIVE = "receive"

    def to_str(self) -> str:
        return str(self.value)


@dataclasses.dataclass(frozen=True)
class InterestRateSwap:
    notional: Decimal

    effective_date: datetime.date
    maturity_date: datetime.date

    fixed_leg: PayReceive

    currency: Currency

    def with_change(
        self,
        *,
        notional: Decimal | None = None,
        effective_date: datetime.date | None = None,
        maturity_date: datetime.date | None = None,
        fixed_leg: PayReceive | None = None,
        currency: Currency | None = None,
    ) -> Self:
        new_notional = notional if notional is not None else self.notional
        new_effective_date = effective_date if effective_date is not None else self.effective_date
        new_maturity_date = maturity_date if maturity_date is not None else self.maturity_date
        new_fixed_leg = fixed_leg if fixed_leg is not None else self.fixed_leg
        new_currency = currency if currency is not None else self.currency

        return self.__class__(
            notional=new_notional,
            effective_date=new_effective_date,
            maturity_date=new_maturity_date,
            fixed_leg=new_fixed_leg,
            currency=new_currency,
        )
    