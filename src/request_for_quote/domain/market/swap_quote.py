from dataclasses import dataclass
from decimal import Decimal
from typing import Self

from request_for_quote.domain.market.identifier import QuoteSetId
from request_for_quote.domain.market.tenor import Tenor
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.domain.shared.error import DomainError


@dataclass(frozen=True, slots=True)
class SwapQuote:
    tenor: Tenor
    par_rate: Decimal


@dataclass(frozen=True, slots=True)
class SwapQuoteSnapshot:
    quote_set_id: QuoteSetId
    as_of: AwareDateTime
    quotes: tuple[SwapQuote, ...]

    def __post_init__(self) -> None:
        if not self.quotes:
            raise InvalidSwapQuoteSnapshot.quotes_are_empty()

        tenors = tuple(quote.tenor for quote in self.quotes)

        if len(tenors) != len(set(tenors)):
            raise InvalidSwapQuoteSnapshot.tenors_are_duplicated()


class InvalidSwapQuoteSnapshot(DomainError):
    @classmethod
    def quotes_are_empty(cls) -> Self:
        msg = "SwapQuoteSnapshot requres at least one quote."
        return cls(msg)

    @classmethod
    def tenors_are_duplicated(cls) -> Self:
        msg = f"SwapQuoteSnapshot must unique tenor."
        return cls(msg)
    