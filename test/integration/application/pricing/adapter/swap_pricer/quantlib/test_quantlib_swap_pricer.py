from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)
from decimal import Decimal

from request_for_quote.application.market.use_case.build_market_curve import BuildMarketCurveUseCase
from request_for_quote.application.pricing.dependency.pricing_dependency_resolver import PricingDependencyResolver
from request_for_quote.application.pricing.model import (
    PricingDependencies,
    ProjectionCurve,
)
from request_for_quote.application.pricing.model.market_configuration import MarketConfiguration, ProjectionCurveConfiguration
from request_for_quote.domain.market import (
    CurveId,
    CurveNode,
    MarketCurve,
)
from request_for_quote.domain.market.identifier import QuoteSetId
from request_for_quote.domain.market.market_curve import CurveInterpolation
from request_for_quote.domain.market.swap_quote import SwapQuote, SwapQuoteSnapshot
from request_for_quote.domain.market.tenor import Tenor, TenorUnit
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
from request_for_quote.domain.product.interest_rate.convention import BusinessCalendarId, BusinessDayConvention, SwapConvention
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.application.market.adapter.curve_builder.quantlib.quantlib_curve_builder import QuantLibCurveBuilder
from request_for_quote.infrastructure.application.market.adapter.market_curve_store.in_memory.market_curve_store import InMemoryMarketCurveStore
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


def _create_swap_quote_snapshot(spread: Decimal = Decimal("0")) -> SwapQuoteSnapshot:
    return SwapQuoteSnapshot(
        quote_set_id=QuoteSetId("JPY-TONA"),
        as_of=AwareDateTime(datetime.now(tz=timezone(timedelta(hours=9)))),
        quotes=(
            SwapQuote(
                tenor=Tenor(value=1, unit=TenorUnit.YEAR),
                par_rate=Decimal("0.01") + spread,
            ),
            SwapQuote(
                tenor=Tenor(value=2, unit=TenorUnit.YEAR),
                par_rate=Decimal("0.011") + spread,
            ),
            SwapQuote(
                tenor=Tenor(value=3, unit=TenorUnit.YEAR),
                par_rate=Decimal("0.013") + spread,
            ),
            SwapQuote(
                tenor=Tenor(value=5, unit=TenorUnit.YEAR),
                par_rate=Decimal("0.02") + spread,
            ),
        ),
    )


def _create_convention() -> SwapConvention:
    return SwapConvention(
        floating_rate_index=FloatingRateIndex("JPY-TONA"),
        fixed_leg_frequency=PeriodFrequency.ANNUAL,
        fixed_leg_day_count=DayCountFraction.ACT_365_FIXED,
        business_calendar_id=BusinessCalendarId("JP"),
        business_day_convention=BusinessDayConvention.MODIFIED_FOLLOWING,
        settlement_days=2,
    )


def _create_market_configuration() -> MarketConfiguration:
    return MarketConfiguration(
        discount_curve_id=CurveId("JPY-OIS"),
        projection_curves=(
            ProjectionCurveConfiguration(
                floating_rate_index=FloatingRateIndex("JPY-TONA"),
                curve_id=CurveId("JPY-OIS"),
            ),
        )
    )


def test_snapshot_update_changes_swap_price() -> None:
    curve_store = InMemoryMarketCurveStore()
    build_curve = BuildMarketCurveUseCase(
        curve_builder=QuantLibCurveBuilder(),
        curve_store=curve_store,
    )
    market_configuration = _create_market_configuration()
    resolver = PricingDependencyResolver(curve_provider=curve_store)
    swap_pricer = _create_pricer()

    swap = _create_swap(fixed_rate=Decimal("0.01"))
    convention = _create_convention()
    initial_snapshot = _create_swap_quote_snapshot()

    build_curve.execute(
        curve_id=CurveId("JPY-OIS"),
        snapshot=initial_snapshot,
        convention=convention,
    )

    initial_result = swap_pricer.price(
        swap=swap,
        dependencies=resolver.resolve(
            product=swap,
            market_data_configuration=market_configuration,
        )
    )

    second_snapshot = _create_swap_quote_snapshot(spread=Decimal("0.001"))
    build_curve.execute(
        curve_id=CurveId("JPY-OIS"),
        snapshot=second_snapshot,
        convention=convention,
    )

    second_result = swap_pricer.price(
        swap=swap,
        dependencies=resolver.resolve(
            product=swap,
            market_data_configuration=market_configuration,
        )
    )

    assert initial_result.fair_rate < second_result.fair_rate
