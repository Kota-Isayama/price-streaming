import datetime

import QuantLib as ql

from request_for_quote.domain.product.interest_rate.common import DayCountFraction, PayerReceiver
from request_for_quote.domain.product.interest_rate.schedule import PeriodFrequency

def to_quantlib_date(value: datetime.date) -> ql.Date:
    return ql.Date(
        value.day,
        value.month,
        value.year,
    )


def to_quantlib_period(
    frequency: PeriodFrequency,
) -> ql.Period:
    match frequency:
        case PeriodFrequency.MONTHLY:
            return ql.Period(
                1,
                ql.Months,
            )

        case PeriodFrequency.QUARTERLY:
            return ql.Period(
                3,
                ql.Months,
            )

        case PeriodFrequency.SEMI_ANNUAL:
            return ql.Period(
                6,
                ql.Months,
            )

        case PeriodFrequency.ANNUAL:
            return ql.Period(
                1,
                ql.Years,
            )

        case _:
            raise ValueError(
                "Unsupported frequency: "
                f"{frequency}"
            )


def to_quantlib_daycount(
    day_count: DayCountFraction,
):  # TODO: Annotate.
    match day_count:
        case DayCountFraction.ACT_360:
            return ql.Actual360()

        case DayCountFraction.ACT_365_FIXED:
            return ql.Actual365Fixed()

        case DayCountFraction.THIRTY_360:
            return ql.Thirty360(
                ql.Thirty360.BondBasis
            )

        case _:
            raise ValueError(
                "Unsupported day count: "
                f"{day_count}"
            )


def to_quantlib_swap_type(
    fixed_leg_payer_receiver: PayerReceiver,
):  # TODO: Annotate
    match fixed_leg_payer_receiver:
        case PayerReceiver.PAYER:
            return ql.Swap.Payer

        case PayerReceiver.RECEIVER:
            return ql.Swap.Receiver

        case _:
            raise ValueError(
                "Unsupported payer/receiver: "
                f"{fixed_leg_payer_receiver}"
            )

