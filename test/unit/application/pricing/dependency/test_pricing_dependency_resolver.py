from datetime import (
    date,
    datetime,
    timezone,
)
from decimal import Decimal

from request_for_quote.application.pricing.dependency import (
    PricingDependencyResolver,
)
from request_for_quote.application.pricing.model import (
    MarketConfiguration,
    ProjectionCurveConfiguration,
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
from request_for_quote.infrastructure.application.market.adapter.market_curve_store.in_memory.market_curve_store import (
    InMemoryMarketCurveStore,
)


def test_resolve_jpy_ois_dependencies() -> None:
    curve_id = CurveId(
        "JPY-OIS"
    )

    curve = MarketCurve(
        curve_id=curve_id,
        reference_date=date(
            2026,
            10,
            11,
        ),
        as_of=AwareDateTime(
            datetime(
                2026,
                10,
                11,
                12,
                0,
                tzinfo=timezone.utc,
            ),
        ),
        nodes=(
            CurveNode(
                pillar_date=date(
                    2026,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "1"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2031,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "0.95"
                ),
            ),
        ),
        interpolation=CurveInterpolation.LOG_LINEAR_DISCOUNT,
    )

    curve_store = (
        InMemoryMarketCurveStore()
    )
    curve_store.put(curve)

    tona = FloatingRateIndex(
        "JPY-TONA"
    )

    configuration = (
        MarketConfiguration(
            discount_curve_id=curve_id,
            projection_curves=(
                ProjectionCurveConfiguration(
                    floating_rate_index=tona,
                    curve_id=curve_id,
                ),
            ),
        )
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
                13,
            ),
            termination_date=date(
                2031,
                10,
                13,
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
                rate=Decimal(
                    "0.01"
                )
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
                index=tona
            )
        ),
    )

    product = InterestRateSwap(
        payouts=(
            fixed_leg,
            floating_leg,
        )
    )

    resolver = (
        PricingDependencyResolver(
            curve_provider=curve_store,
        )
    )

    dependencies = resolver.resolve(
        product=product,
        market_data_configuration=(
            configuration
        ),
    )

    assert (
        dependencies.discount_curve
        is curve
    )

    assert (
        dependencies
        .projection_curve_for(tona)
        is curve
    )
