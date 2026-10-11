from contextlib import contextmanager
from decimal import Decimal
from typing import Generator

import QuantLib as ql

from request_for_quote.application.pricing.model.pricing_dependencies import PricingDependencies
from request_for_quote.application.pricing.model.pricing_result import PricingResult
from request_for_quote.application.pricing.port.swap_pricer import ISwapPricer
from request_for_quote.domain.product.interest_rate.interest_rate_swap import InterestRateSwap
from request_for_quote.infrastructure.application.pricing.adapter.swap_pricer.quantlib.quantlib_curve_factory import QuantLibCurveFactory
from request_for_quote.infrastructure.application.pricing.adapter.swap_pricer.quantlib.quantlib_swap_factory import QuantLibSwapFactory
from request_for_quote.infrastructure.quantlib.util import to_quantlib_date


class QuantLibSwapPricer(
    ISwapPricer
):
    def __init__(
        self,
        *,
        curve_factory: QuantLibCurveFactory,
        swap_factory: QuantLibSwapFactory,
    ) -> None:
        self._curve_factory = curve_factory
        self._swap_factory = swap_factory

    def price(
        self,
        *,
        swap: InterestRateSwap,
        dependencies: PricingDependencies,
    ) -> PricingResult:
        evaluation_date = dependencies.discount_curve.reference_date  # TODO: Define pricing context or something like that.

        with self._evaluation_date(to_quantlib_date(evaluation_date)):
            discount_curve = self._curve_factory.build(dependencies.discount_curve)
            discount_handle = ql.YieldTermStructureHandle(discount_curve)

            quantlib_swap = self._swap_factory.build(swap=swap, dependencies=dependencies)

            quantlib_swap.setPricingEngine(ql.DiscountingSwapEngine(discount_handle))
            npv = quantlib_swap.NPV()
            par_rate = quantlib_swap.fairRate()

        return PricingResult(
            npv=Decimal(str(npv)),
            fair_rate=Decimal(str(par_rate)),
        )


    @staticmethod
    @contextmanager
    def _evaluation_date(
        value: ql.Date,
    ) -> Generator[None]:
        settings = (
            ql.Settings.instance()
        )

        previous = (
            settings.evaluationDate
        )

        settings.evaluationDate = value

        try:
            yield

        finally:
            settings.evaluationDate = (
                previous
            )
            