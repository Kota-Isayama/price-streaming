import QuantLib as ql

from request_for_quote.application.pricing.model.pricing_dependencies import PricingDependencies
from request_for_quote.domain.product.interest_rate.common import Currency
from request_for_quote.domain.product.interest_rate.interest_rate_payout import InterestRatePayout
from request_for_quote.domain.product.interest_rate.interest_rate_swap import InterestRateSwap
from request_for_quote.domain.product.interest_rate.rate_specification import FixedRateSpecification, FloatingRateSpecification
from request_for_quote.infrastructure.application.pricing.adapter.swap_pricer.quantlib.quantlib_curve_factory import QuantLibCurveFactory
from request_for_quote.infrastructure.quantlib.util import to_quantlib_date, to_quantlib_daycount, to_quantlib_period, to_quantlib_swap_type


class QuantLibSwapFactory:
    def __init__(
        self,
        *,
        curve_factory: QuantLibCurveFactory,
    ) -> None:
        self._curve_factory = curve_factory

    def build(
        self,
        *,
        swap: InterestRateSwap,
        dependencies: PricingDependencies,
    ) -> ...:
        fixed_leg, floating_leg = self._extract_legs(swap)

        self._validate(
            fixed_leg=fixed_leg,
            floating_leg=floating_leg,
        )

        fixed_spacification = fixed_leg.rate_specification
        floating_specification = floating_leg.rate_specification

        assert isinstance(fixed_spacification, FixedRateSpecification)
        assert isinstance(floating_specification, FloatingRateSpecification)

        # 何をしているのか、QuantLibの理解が必要そう
        projection_curve = dependencies.projection_curve_for(floating_specification.index)

        projection_term_structure = self._curve_factory.build(projection_curve)

        projection_handle = ql.YieldTermStructureHandle(projection_term_structure)

        overnight_index = self._overnight_index(index_name=floating_specification.index.value, projection_handle=projection_handle)

        calendar = overnight_index.fixingCalendar()

        schedule = ql.Schedule(
            to_quantlib_date(fixed_leg.calculation_period_dates.effective_date),
            to_quantlib_date(fixed_leg.calculation_period_dates.termination_date),
            to_quantlib_period(fixed_leg.calculation_period_dates.frequency),
            calendar,
            ql.ModifiedFollowing,
            ql.ModifiedFollowing,
            ql.DateGeneration.Forward,
            False,
        )

        swap_type = to_quantlib_swap_type(fixed_leg.payer_receiver)  # TODO: どっち視点なのか結局分からなくない？ Payerだったとして、どっちがPayerなのか？

        notional = float(fixed_leg.notional_schedule.initial_value.amount)
        fixed_rate = float(fixed_spacification.rate)
        fixed_day_count = to_quantlib_daycount(fixed_leg.day_count_fraction)
        spread = float(floating_specification.spread)
        payment_lag = fixed_leg.payment_dates.payment_lag_business_days

        return ql.OvernightIndexedSwap(
            swap_type,
            notional,
            schedule,
            fixed_rate,
            fixed_day_count,
            overnight_index,
            spread,
            payment_lag,
            ql.ModifiedFollowing,  # TODO: Get this information from argument.
            calendar,
        )


    def _extract_legs(self, swap: InterestRateSwap) -> tuple[InterestRatePayout, InterestRatePayout]:
        fixed_legs = [
            payout
            for payout in swap.payouts
            if isinstance(payout.rate_specification, FixedRateSpecification)
        ]

        floating_legs = [
            payout
            for payout in swap.payouts
            if isinstance(payout.rate_specification, FloatingRateSpecification)
        ]

        if len(fixed_legs) != 1 or len(floating_legs) != 1:
            raise ValueError(
                "QuantLibSwapFactory currently supports "
                "exactly one fixed leg and one "
                "floating leg"
            )

        return fixed_legs[0], floating_legs[0]

    @staticmethod
    def _validate(
        *,
        fixed_leg: InterestRatePayout,
        floating_leg: InterestRatePayout,
    ) -> None:
        fixed_notional = (
            fixed_leg
            .notional_schedule
            .initial_value
        )

        floating_notional = (
            floating_leg
            .notional_schedule
            .initial_value
        )

        if (
            fixed_notional.currency
            != Currency.JPY
            or floating_notional.currency
            != Currency.JPY
        ):
            raise ValueError(
                "Only JPY OIS is currently supported"
            )

        if (
            fixed_notional.amount
            != floating_notional.amount
        ):
            raise ValueError(
                "Different leg notionals are "
                "not yet supported"
            )

        if (
            fixed_leg.payer_receiver
            == floating_leg.payer_receiver
        ):
            raise ValueError(
                "Fixed and floating legs must "
                "have opposite payer/receiver"
            )

        fixed_period = (
            fixed_leg.calculation_period_dates
        )
        floating_period = (
            floating_leg.calculation_period_dates
        )

        if (
            fixed_period.effective_date
            != floating_period.effective_date
            or fixed_period.termination_date
            != floating_period.termination_date
        ):
            raise ValueError(
                "Different fixed/floating leg dates "
                "are not yet supported"
            )

        if (
            fixed_period.frequency
            != floating_period.frequency
        ):
            raise ValueError(
                "Different fixed/floating schedules "
                "are not yet supported"
            )

        if (
            fixed_leg.payment_dates.frequency
            != fixed_period.frequency
            or floating_leg.payment_dates.frequency
            != floating_period.frequency
        ):
            raise ValueError(
                "Payment frequency differing from "
                "calculation frequency is not yet supported"
            )

        if (
            fixed_leg
            .payment_dates
            .payment_lag_business_days
            != floating_leg
            .payment_dates
            .payment_lag_business_days
        ):
            raise ValueError(
                "Different payment lags are "
                "not yet supported"
            )
        
    @staticmethod
    def _overnight_index(
        *,
        index_name: str,
        projection_handle: ql.YieldTermStructureHandle,
    ):
        match index_name:
            case "JPY-TONA":
                return ql.Tonar(
                    projection_handle
                )

            case _:
                raise ValueError(
                    "Unsupported overnight index: "
                    f"{index_name}"
                )
            