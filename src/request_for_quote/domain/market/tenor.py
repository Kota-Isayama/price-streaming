from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from request_for_quote.domain.shared.error import DomainError


class TenorUnit(StrEnum):
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
    YEAR = "Y"


@dataclass(frozen=True)
class Tenor:
    value: int
    unit: TenorUnit

    def __post_init__(self) -> None:
        if self.value < 0:
            raise InvalidTenor.tenor_is_negative(self.value)
        

class InvalidTenor(DomainError):
    @classmethod
    def tenor_is_negative(cls, value: int) -> Self:
        msg = f"Tenor value must not be negative, but {value=}."
        return cls(msg)