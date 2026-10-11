from request_for_quote.domain.market import (
    QuoteSetId,
    SwapQuote,
    SwapQuoteSnapshot,
    Tenor,
    TenorUnit,
)
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import (
    SwapQuoteSnapshotMessage,
)


class SwapQuoteSnapshotMapper:
    @staticmethod
    def to_domain(
        message: SwapQuoteSnapshotMessage,
    ) -> SwapQuoteSnapshot:
        return SwapQuoteSnapshot(
            quote_set_id=QuoteSetId(
                message.quote_set_id
            ),
            as_of=AwareDateTime(message.as_of),
            quotes=tuple(
                SwapQuote(
                    tenor=Tenor(
                        value=(
                            quote.tenor.value
                        ),
                        unit=TenorUnit(
                            quote.tenor.unit
                        ),
                    ),
                    par_rate=(
                        quote.par_rate
                    ),
                )
                for quote
                in message.quotes
            ),
        )
    