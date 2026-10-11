from dataclasses import dataclass
from typing import Self

from request_for_quote.domain.product.interest_rate.interest_rate_payout import InterestRatePayout
from request_for_quote.domain.shared.error import DomainError


@dataclass(frozen=True)
class InterestRateSwap:
    payouts: tuple[InterestRatePayout, ...]

    def __post_init__(self) -> None:
        if len(self.payouts) < 2:
            raise InvalidInterestRateSwap.the_number_of_payouts_is_insufficient()
        

class InvalidInterestRateSwap(DomainError):
    @classmethod
    def the_number_of_payouts_is_insufficient(cls) -> Self:
        msg = f"InterestRateSwap requires at least two payouts."
        return cls(msg)
    