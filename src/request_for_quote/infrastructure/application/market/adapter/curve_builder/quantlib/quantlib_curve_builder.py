from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from email import contentmanager
from typing import Generator

import QuantLib as ql
from sqlalchemy.schema import conv

from request_for_quote.application.market.port.curve_builder import ICurveBuilder
from request_for_quote.domain.market.identifier import CurveId
from request_for_quote.domain.market.market_curve import CurveInterpolation, CurveNode, MarketCurve
from request_for_quote.domain.market.swap_quote import SwapQuoteSnapshot
from request_for_quote.domain.product.interest_rate.common import DayCountFraction
from request_for_quote.domain.product.interest_rate.convention import SwapConvention
from request_for_quote.infrastructure.application.market.adapter.curve_builder.quantlib.convention_mapper import QuantLibConventionMapper


class QuantLibCurveBuilder(
    ICurveBuilder
):
    def build(
        self,
        *,
        curve_id: CurveId,
        snapshot: SwapQuoteSnapshot,
        convention: SwapConvention,
    ) -> MarketCurve:
        self._validate_convention(convention=convention)

        reference_date = snapshot.as_of.value.date()
        ql_reference_date = self._to_quantlib_date(reference_date)

        with self._evaluation_date(ql_reference_date):
            curve = self._build_curve(
                reference_date=ql_reference_date,
                snapshot=snapshot,
                convention=convention,
            )

            nodes = self._to_domain_nodes(curve)

        return MarketCurve(
            curve_id=curve_id,
            reference_date=reference_date,
            as_of=snapshot.as_of,
            nodes=nodes,
            interpolation=CurveInterpolation.LOG_LINEAR_DISCOUNT,
        )

    def _build_curve(
        self,
        *,
        reference_date: ql.Date,
        snapshot: SwapQuoteSnapshot,
        convention: SwapConvention,
    ) -> ...:
        calendar = QuantLibConventionMapper.calendar(convention.business_calendar_id)
        business_day_convention = QuantLibConventionMapper.business_day_convention(convention.business_day_convention)
        fixed_frequency = QuantLibConventionMapper.frequency(convention.fixed_leg_frequency)
        overnight_index = QuantLibConventionMapper.overnight_index(convention.floating_rate_index)

        helpers = [
            ql.OISRateHelper(
                settlementDays=convention.settlement_days,
                tenor=QuantLibConventionMapper.tenor(quote.tenor),
                rate=float(quote.par_rate),
                index=overnight_index,
                paymentConvention=business_day_convention,
                paymentFrequency=fixed_frequency,
                paymentCalendar=calendar,
                fixedPaymentFrequency=fixed_frequency,
                fixedCalendar=calendar,
                overnightCalendar=calendar,
                convention=business_day_convention,
            ) for quote in snapshot.quotes
        ]

        curve = ql.PiecewiseLogLinearDiscount(reference_date, helpers, ql.Actual365Fixed())

        # QuantLibのbootstrapはlazyなので
        # ここで一度計算を強制する
        curve.discount(curve.maxDate())

        return curve

    def _to_domain_nodes(self, curve: ql.PiecewiseLogLinearDiscount) -> tuple[CurveNode, ...]:
        nodes: list[CurveNode] = []

        for (ql_date, discount_factor) in curve.nodes():
            nodes.append(
                CurveNode(
                    pillar_date=self._to_python_date(ql_date),
                    discount_factor=Decimal(str(discount_factor)),
                )
            )

        return tuple(nodes)

    def _validate_convention(self, convention: SwapConvention) -> None:
        if convention.floating_rate_index.value == "JPY-TONA" and convention.fixed_leg_day_count != DayCountFraction.ACT_365_FIXED:
            raise ValueError(
                "QuantLib 1.43 OISRateHelper "
                "cannot override fixed-leg "
                "day count. "
                "JPY-TONA is currently "
                "supported only with ACT/365F."
            )

    @staticmethod
    def _to_quantlib_date(
        value: date,
    ) -> ql.Date:
        return ql.Date(
            value.day,
            value.month,
            value.year,
        )

    @staticmethod
    def _to_python_date(
        value: ql.Date,
    ) -> date:
        return date(
            value.year(),
            int(value.month()),
            value.dayOfMonth(),
        )

    @staticmethod
    @contextmanager
    def _evaluation_date(value: ql.Date) -> Generator[None]:
        settings = ql.Settings.instance()
        previous = settings.evaluationDate

        settings.evaluationDate = value

        try:
            yield
        finally:
            settings.evaluationDate = previous
