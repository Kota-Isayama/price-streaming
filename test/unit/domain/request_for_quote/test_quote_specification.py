import pytest

from request_for_quote.domain.request_for_quote.quote_specification import InvalidQuoteSpecification, QuoteFieldId, QuoteSpecification


def test_at_least_one_quote_target_must_be_required() -> None:
    with pytest.raises(InvalidQuoteSpecification):
        QuoteSpecification(
            targets=tuple(),
        )


def test_quote_targets_must_not_be_duplicated() -> None:
    with pytest.raises(InvalidQuoteSpecification):
        QuoteSpecification(
            targets=(
                QuoteFieldId("a"),
                QuoteFieldId("a"),
            )
        )
