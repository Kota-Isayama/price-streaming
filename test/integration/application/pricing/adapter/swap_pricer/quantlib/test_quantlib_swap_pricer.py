from datetime import (
    date,
    datetime,
    timezone,
)
from decimal import Decimal

from request_for_quote.application.pricing.model import (
    PricingDependencies,
    ProjectionCurve,
)
from request_for_quote.domain.market import (
    CurveId,
    CurveNode,
    MarketCurve,
)
from request_for_quote.domain.market.market_curve import CurveInterpolation
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
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.application.pricing.adapter.swap_pricer.quantlib import (
    QuantLibCurveFactory,
    QuantLibSwapFactory,
    QuantLibSwapPricer,
)


def _create_curve() -> MarketCurve:
    return MarketCurve(
        curve_id=CurveId(
            "JPY-OIS"
        ),
        reference_date=date(
            2026,
            10,
            13,
        ),
        as_of=AwareDateTime(
            datetime(
                2026,
                10,
                13,
                0,
                0,
                tzinfo=timezone.utc,
            ),
        ),
        nodes=(
            CurveNode(
                pillar_date=date(
                    2026,
                    10,
                    13,
                ),
                discount_factor=Decimal(
                    "1"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2027,
                    10,
                    13,
                ),
                discount_factor=Decimal(
                    "0.990"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2031,
                    10,
                    13,
                ),
                discount_factor=Decimal(
                    "0.950"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2032,
                    10,
                    13,
                ),
                discount_factor=Decimal(
                    "0.938"
                ),
            ),
        ),
        interpolation=CurveInterpolation.LOG_LINEAR_DISCOUNT,
    )


def _create_swap(
    *,
    fixed_rate: Decimal,
) -> InterestRateSwap:
    tona = FloatingRateIndex(
        "JPY-TONA"
    )

    notional = NotionalSchedule(
        initial_value=Money(
            amount=Decimal(
                "100000000"
            ),
            currency=Currency.JPY,
        )
    )

    calculation_period_dates = (
        CalculationPeriodDates(
            effective_date=date(
                2026,
                10,
                14,
            ),
            termination_date=date(
                2031,
                10,
                14,
            ),
            frequency=(
                PeriodFrequency.ANNUAL
            ),
        )
    )

    payment_dates = PaymentDates(
        frequency=PeriodFrequency.ANNUAL
    )

    fixed_leg = InterestRatePayout(
        payer_receiver=(
            PayerReceiver.PAYER
        ),
        notional_schedule=notional,
        calculation_period_dates=(
            calculation_period_dates
        ),
        payment_dates=payment_dates,
        day_count_fraction=(
            DayCountFraction.ACT_365_FIXED
        ),
        rate_specification=(
            FixedRateSpecification(
                rate=fixed_rate
            )
        ),
    )

    floating_leg = InterestRatePayout(
        payer_receiver=(
            PayerReceiver.RECEIVER
        ),
        notional_schedule=notional,
        calculation_period_dates=(
            calculation_period_dates
        ),
        payment_dates=payment_dates,
        day_count_fraction=(
            DayCountFraction.ACT_365_FIXED
        ),
        rate_specification=(
            FloatingRateSpecification(
                index=tona,
                spread=Decimal("0"),
            )
        ),
    )

    return InterestRateSwap(
        payouts=(
            fixed_leg,
            floating_leg,
        )
    )


def _create_dependencies(
    curve: MarketCurve,
) -> PricingDependencies:
    return PricingDependencies(
        discount_curve=curve,
        projection_curves=(
            ProjectionCurve(
                floating_rate_index=(
                    FloatingRateIndex(
                        "JPY-TONA"
                    )
                ),
                curve=curve,
            ),
        ),
    )


def _create_pricer() -> QuantLibSwapPricer:
    curve_factory = (
        QuantLibCurveFactory()
    )

    swap_factory = QuantLibSwapFactory(
        curve_factory=curve_factory
    )

    return QuantLibSwapPricer(
        curve_factory=curve_factory,
        swap_factory=swap_factory,
    )


def test_price_jpy_ois_swap() -> None:
    curve = _create_curve()

    dependencies = (
        _create_dependencies(
            curve
        )
    )

    swap = _create_swap(
        fixed_rate=Decimal(
            "0.01"
        )
    )

    result = (
        _create_pricer()
        .price(
            swap=swap,
            dependencies=dependencies,
        )
    )

    assert result.fair_rate > 0


def test_npv_is_zero_at_par_rate() -> None:
    curve = _create_curve()

    dependencies = (
        _create_dependencies(
            curve
        )
    )

    pricer = _create_pricer()

    initial_swap = _create_swap(
        fixed_rate=Decimal(
            "0.01"
        )
    )

    initial_result = pricer.price(
        swap=initial_swap,
        dependencies=dependencies,
    )

    par_swap = _create_swap(
        fixed_rate=(
            initial_result.fair_rate
        )
    )

    par_result = pricer.price(
        swap=par_swap,
        dependencies=dependencies,
    )

    assert abs(
        par_result.npv
    ) < Decimal("0.01")
