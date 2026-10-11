from dataclasses import dataclass
from typing import Self

from request_for_quote.domain.shared.error import DomainError


@dataclass(frozen=True)
class CurveId:
    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise InvalidCurveId.id_is_empty()

    def to_str(self) -> str:
        return self.value


class InvalidCurveId(DomainError):
    @classmethod
    def id_is_empty(cls) -> Self:
        msg = f"Market ID is empty."
        return cls(msg)


@dataclass(frozen=True)
class QuoteSetId:
    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise InvalidQuoteSetId.id_is_empty()


class InvalidQuoteSetId(DomainError):
    @classmethod
    def id_is_empty(cls) -> Self:
        msg = f"QuoteSet ID is empty."
        return cls(msg)
