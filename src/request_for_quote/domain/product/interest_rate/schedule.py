from dataclasses import dataclass
import datetime
import enum
from typing import Self

from request_for_quote.domain.shared.error import DomainError


class PeriodFrequency(enum.StrEnum):
    MONTHLY = "1M"
    QUARTERLY = "3M"
    SEMI_ANNUAL = "6M"
    ANNUAL = "1Y"


@dataclass(frozen=True)
class CalculationPeriodDates:
    effective_date: datetime.date
    termination_date: datetime.date
    frequency: PeriodFrequency

    def __post_init__(self) -> None:
        if self.effective_date >= self.termination_date:
            raise InvalidCalculationPeriodDates.effective_date_is_after_termination_date(self.effective_date, self.termination_date)


class InvalidCalculationPeriodDates(DomainError):
    @classmethod
    def effective_date_is_after_termination_date(cls, effective_date: datetime.date, termination_date: datetime.date) -> Self:
        msg = f"effective_date must be before termination_date, but {effective_date=} and {termination_date=}"
        return cls(msg)


@dataclass(frozen=True, slots=True)
class PaymentDates:
    frequency: PeriodFrequency
    payment_lag_business_days: int = 0

    def __post_init__(self) -> None:
        if self.payment_lag_business_days < 0:
            raise ValueError(
                "payment_lag_business_days "
                "must not be negative"
            )


class InvalidPaymentDates(DomainError):
    frequency: PeriodFrequency
    payment_lag_business_days: int
    