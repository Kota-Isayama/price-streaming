from dataclasses import dataclass

from request_for_quote.domain.product.interest_rate.common import DayCountFraction, PayerReceiver
from request_for_quote.domain.product.interest_rate.notional import NotionalSchedule
from request_for_quote.domain.product.interest_rate.rate_specification import RateSpecification
from request_for_quote.domain.product.interest_rate.schedule import CalculationPeriodDates, PaymentDates


@dataclass(frozen=True)
class InterestRatePayout:
    payer_receiver: PayerReceiver

    notional_schedule: NotionalSchedule

    calculation_period_dates: CalculationPeriodDates
    payment_dates: PaymentDates

    day_count_fraction: DayCountFraction
    rate_specification: RateSpecification
    