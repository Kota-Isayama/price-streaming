import pytest

from request_for_quote.domain.product.interest_rate import (
    BusinessCalendarId,
    BusinessDayConvention,
    DayCountFraction,
    FloatingRateIndex,
    PeriodFrequency,
    SwapConvention,
)
from request_for_quote.domain.product.interest_rate.convention import InvalidSwapConvention


def test_create_swap_convention() -> None:
    convention = SwapConvention(
        floating_rate_index=FloatingRateIndex(
            "JPY-TONA"
        ),
        fixed_leg_frequency=PeriodFrequency.ANNUAL,
        fixed_leg_day_count=DayCountFraction.ACT_365_FIXED,
        business_calendar_id=BusinessCalendarId(
            "JP"
        ),
        business_day_convention=(
            BusinessDayConvention.MODIFIED_FOLLOWING
        ),
        settlement_days=2,
    )

    assert (
        convention.floating_rate_index.value
        == "JPY-TONA"
    )


def test_negative_settlement_days_is_rejected() -> None:
    with pytest.raises(InvalidSwapConvention):
        SwapConvention(
            floating_rate_index=FloatingRateIndex(
                "JPY-TONA"
            ),
            fixed_leg_frequency=PeriodFrequency.ANNUAL,
            fixed_leg_day_count=(
                DayCountFraction.ACT_365_FIXED
            ),
            business_calendar_id=BusinessCalendarId(
                "JP"
            ),
            business_day_convention=(
                BusinessDayConvention.MODIFIED_FOLLOWING
            ),
            settlement_days=-1,
        )
