import datetime
from decimal import Decimal

from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_mapper import SwapQuoteSnapshotMapper
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import SwapQuoteMessage, SwapQuoteSnapshotMessage, TenorMessage


def test_swap_quote_snapshot_message_can_be_mapped_to_domain_model() -> None:
    message = _create_swap_quote_snapshot_message()

    SwapQuoteSnapshotMapper.to_domain(message=message)


def _create_swap_quote_snapshot_message() -> SwapQuoteSnapshotMessage:
    return SwapQuoteSnapshotMessage(
        quote_set_id="JPY-TONA",
        as_of=datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=9))),
        quotes=(
            SwapQuoteMessage(
                tenor=TenorMessage(value=1, unit="Y"),
                par_rate=Decimal("0.01"),
            ),
            SwapQuoteMessage(
                tenor=TenorMessage(value=2, unit="Y"),
                par_rate=Decimal("0.011"),
            ),
            SwapQuoteMessage(
                tenor=TenorMessage(value=3, unit="Y"),
                par_rate=Decimal("0.013"),
            ),
            SwapQuoteMessage(
                tenor=TenorMessage(value=5, unit="Y"),
                par_rate=Decimal("0.02"),
            ),
        ),
    )