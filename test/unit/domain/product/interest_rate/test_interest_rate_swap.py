from datetime import date
from decimal import Decimal

from request_for_quote.domain.product.interest_rate import (
    CalculationPeriodDates,
    Currency,
    DayCountFraction,
    FixedRateSpecification,
    FloatingRateIndex,
    FloatingRateSpecification,
    InterestRatePayout,
    InterestRateSwap,
    Money,
    NotionalSchedule,
    PayerReceiver,
    PaymentDates,
    PeriodFrequency,
)


def test_create_jpy_ois_swap() -> None:
    notional = NotionalSchedule(
        initial_value=Money(
            amount=Decimal("100000000"),
            currency=Currency.JPY,
        )
    )

    calculation_period_dates = CalculationPeriodDates(
        effective_date=date(
            2026,
            10,
            12,
        ),
        termination_date=date(
            2031,
            10,
            12,
        ),
        frequency=PeriodFrequency.ANNUAL,
    )

    payment_dates = PaymentDates(
        frequency=PeriodFrequency.ANNUAL,
    )

    fixed_leg = InterestRatePayout(
        payer_receiver=PayerReceiver.PAYER,
        notional_schedule=notional,
        calculation_period_dates=calculation_period_dates,
        payment_dates=payment_dates,
        day_count_fraction=DayCountFraction.ACT_365_FIXED,
        rate_specification=FixedRateSpecification(
            rate=Decimal("0.0125")
        ),
    )

    floating_leg = InterestRatePayout(
        payer_receiver=PayerReceiver.RECEIVER,
        notional_schedule=notional,
        calculation_period_dates=calculation_period_dates,
        payment_dates=payment_dates,
        day_count_fraction=DayCountFraction.ACT_365_FIXED,
        rate_specification=FloatingRateSpecification(
            index=FloatingRateIndex(
                "JPY-TONA"
            )
        ),
    )

    swap = InterestRateSwap(
        payouts=(
            fixed_leg,
            floating_leg,
        )
    )

    assert len(swap.payouts) == 2
    