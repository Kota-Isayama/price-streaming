from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from request_for_quote.domain.product.interest_rate.common import DayCountFraction
from request_for_quote.domain.product.interest_rate.rate_specification import FloatingRateIndex
from request_for_quote.domain.product.interest_rate.schedule import PeriodFrequency
from request_for_quote.domain.shared.error import DomainError


@dataclass(frozen=True)
class BusinessCalendarId:
    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise InvalidBusinessDayCalendarId.id_is_empty()


class InvalidBusinessDayCalendarId(DomainError):
    @classmethod
    def id_is_empty(cls) -> Self:
        msg = f"BusinessCalendar ID is empty."
        return cls(msg)


class BusinessDayConvention(StrEnum):
    FOLLOWING = "following"
    MODIFIED_FOLLOWING = "modified_following"
    PRECEDING = "preceding"
    MODIFIED_PRECEDING = "modified_preceding"
    UNADJUSTED = "unadjusted"


@dataclass(frozen=True, slots=True)
class SwapConvention:
    """
    Market swap quoteを解釈するための標準契約条件。
    
    例えば
        5Y = 1.00%
    というquoteだけではSwapを一意に定義できないため、
    Curve bootstrap時にこのConventionを組み合わせる。
    """
    floating_rate_index: FloatingRateIndex

    fixed_leg_frequency: PeriodFrequency
    fixed_leg_day_count: DayCountFraction

    business_calendar_id: BusinessCalendarId
    business_day_convention: BusinessDayConvention

    settlement_days: int

    def __post_init__(self) -> None:
        if self.settlement_days < 0:
            raise InvalidSwapConvention.settlement_days_is_negative(self.settlement_days)


class InvalidSwapConvention(DomainError):
    @classmethod
    def settlement_days_is_negative(cls, settlement_days: int) -> Self:
        msg = f"Settlement days must not be negative, but {settlement_days}."
        return cls(msg)
    