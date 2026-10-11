from typing import Any

import QuantLib as ql

from request_for_quote.domain.market.tenor import Tenor, TenorUnit
from request_for_quote.domain.product.interest_rate.common import DayCountFraction
from request_for_quote.domain.product.interest_rate.convention import BusinessCalendarId, BusinessDayConvention
from request_for_quote.domain.product.interest_rate.rate_specification import FloatingRateIndex
from request_for_quote.domain.product.interest_rate.schedule import PeriodFrequency


class QuantLibConventionMapper:
    @staticmethod
    def tenor(
        tenor: Tenor
    ) -> ql.Period:
        match tenor.unit:
            case TenorUnit.DAY:
                unit = ql.Days
            case TenorUnit.WEEK:
                unit = ql.Weeks
            case TenorUnit.MONTH:
                unit = ql.Months
            case TenorUnit.YEAR:
                unit = ql.Years
            case _:
                raise ValueError(
                    f"Unsupported tenor unit: {tenor.unit}"
                )
        return ql.Period(tenor.value, unit)


    @staticmethod
    def frequency(
        frequency: PeriodFrequency,
    ) -> int:
        match frequency:
            case PeriodFrequency.MONTHLY:
                return ql.Monthly
            case PeriodFrequency.QUARTERLY:
                return ql.Quarterly
            case PeriodFrequency.SEMI_ANNUAL:
                return ql.Semiannual
            case PeriodFrequency.ANNUAL:
                return ql.Annual
            case _:
                raise ValueError(
                    "Unsupported period frequency: "
                    f"{frequency}"
                )

    @staticmethod
    def business_day_convention(
        convention: BusinessDayConvention,
    ) -> int:
        match convention:
            case BusinessDayConvention.FOLLOWING:
                return ql.Following
            case BusinessDayConvention.MODIFIED_FOLLOWING:
                return ql.ModifiedFollowing
            case BusinessDayConvention.MODIFIED_PRECEDING:
                return ql.ModifiedPreceding
            case BusinessDayConvention.PRECEDING:
                return ql.Preceding
            case BusinessDayConvention.UNADJUSTED:
                return ql.Unadjusted
            case _:
                raise ValueError(
                    "Unsupported business day convention: "
                    f"{convention}"
                )

    @staticmethod
    def calendar(
        calendar_id: BusinessCalendarId,
    ) -> Any:
        match calendar_id.value:
            case "JP":
                return ql.Japan()
            case _:
                return ValueError(
                    "Unsupported business calendar: "
                    f"{calendar_id.value}"
                )

    @staticmethod
    def day_count(
        day_count: DayCountFraction,
    ) -> Any:
        match day_count:
            case DayCountFraction.ACT_360:
                return ql.Actual360()
            case DayCountFraction.ACT_365_FIXED:
                return ql.Actual365Fixed()
            case DayCountFraction.THIRTY_360:
                return ql.Thirty360(ql.Thirty360.BondBasis)  # これはなんでしたっけ
            case _:
                raise ValueError(
                    "Unsupported day count fraction: "
                    f"{day_count}"
                )

    @staticmethod
    def overnight_index(
        index: FloatingRateIndex,
    ) -> Any:
        match index.value:
            # QuantLibではJPY, JapanCalendar, ACT365F固定になっているらしいので、こちらがわのdomainの設定値との整合性の確認があった方がいいのでは？
            case "JPY-TONA":
                return ql.Tonar()
            case _:
                raise ValueError(
                    "Unsupported floating rate index: "
                    f"{index.value}"
                )
            