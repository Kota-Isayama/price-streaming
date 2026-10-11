from datetime import (
    datetime,
    timezone,
)
from decimal import Decimal

import pytest

from request_for_quote.domain.market import (
    QuoteSetId,
    SwapQuote,
    SwapQuoteSnapshot,
    Tenor,
    TenorUnit,
)
from request_for_quote.domain.market.swap_quote import InvalidSwapQuoteSnapshot
from request_for_quote.domain.shared.aware_datetime import AwareDateTime


def test_create_swap_quote_snapshot() -> None:
    snapshot = SwapQuoteSnapshot(
        quote_set_id=QuoteSetId(
            "JPY-OIS-SWAP-QUOTES"
        ),
        as_of=AwareDateTime(
            datetime(
                2026,
                10,
                11,
                12,
                0,
                tzinfo=timezone.utc,
            ),
        ),
        quotes=(
            SwapQuote(
                tenor=Tenor(
                    1,
                    TenorUnit.YEAR,
                ),
                par_rate=Decimal(
                    "0.007"
                ),
            ),
            SwapQuote(
                tenor=Tenor(
                    5,
                    TenorUnit.YEAR,
                ),
                par_rate=Decimal(
                    "0.010"
                ),
            ),
        ),
    )

    assert len(snapshot.quotes) == 2


def test_duplicate_tenor_is_rejected() -> None:
    with pytest.raises(InvalidSwapQuoteSnapshot):
        SwapQuoteSnapshot(
            quote_set_id=QuoteSetId(
                "JPY-OIS-SWAP-QUOTES"
            ),
            as_of=AwareDateTime(
                datetime.now(
                    timezone.utc
                ),
            ),
            quotes=(
                SwapQuote(
                    tenor=Tenor(
                        5,
                        TenorUnit.YEAR,
                    ),
                    par_rate=Decimal(
                        "0.010"
                    ),
                ),
                SwapQuote(
                    tenor=Tenor(
                        5,
                        TenorUnit.YEAR,
                    ),
                    par_rate=Decimal(
                        "0.011"
                    ),
                ),
            ),
        )