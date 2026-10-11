import enum
from dataclasses import dataclass
from decimal import Decimal
from typing import Self

from request_for_quote.domain.shared.error import DomainError

class Currency(enum.StrEnum):
    JPY = "JPY"


class PayerReceiver(enum.StrEnum):
    PAYER = "payer"
    RECEIVER = "receiver"


class DayCountFraction(enum.StrEnum):
    ACT_360 = "ACT/360"
    ACT_365_FIXED = "ACT/365F"
    THIRTY_360 = "30/360"


@dataclass(frozen=True, slots=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise InvalidMoneyError.amount_is_negative(self.amount)

        
class InvalidMoneyError(DomainError):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)

    @classmethod
    def amount_is_negative(cls, amount: Decimal) -> Self:
        msg = f"Amount must not be negative, but {amount}."
        return cls(msg)
