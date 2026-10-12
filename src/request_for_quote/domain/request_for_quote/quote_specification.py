from dataclasses import dataclass
from typing import Self

from request_for_quote.domain.shared.error import DomainError


@dataclass(frozen=True)
class QuoteFieldId:
    value: str


@dataclass(frozen=True)
class QuoteSpecification:
    targets: tuple[QuoteFieldId, ...]


    def __post_init__(self) -> None:
        if not self.targets:
            raise InvalidQuoteSpecification.target_is_empty()
        if len(self.targets) != len(set(self.targets)):
            raise InvalidQuoteSpecification.targets_are_duplicated()
        

class InvalidQuoteSpecification(DomainError):
    @classmethod
    def target_is_empty(cls) -> Self:
        msg = f"QuoteSpecification requires at least one quote target."
        return cls(msg)

    @classmethod
    def targets_are_duplicated(cls) -> Self:
        msg = f"QuoteSpecification must have unique targets."
        return cls(msg)
    