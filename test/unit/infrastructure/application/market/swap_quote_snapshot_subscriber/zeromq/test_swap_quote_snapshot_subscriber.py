import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import zmq
import zmq.asyncio

from request_for_quote.infrastructure.application.market.adapter.swap_quote_snapshot_subscriber.zeromq.swap_quote_snapshot_subscriber import ZeroMqSwapQuoteSnapshotSubscriber
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import SwapQuoteMessage, SwapQuoteSnapshotMessage, TenorMessage


@pytest.mark.asyncio
async def test_skip_invalid_message() -> None:
    socket = MagicMock(spec=zmq.asyncio.Socket)
    socket.recv_multipart = AsyncMock(
        side_effect=[
            (b"JPY-OIS", b"invalid-json"),
            (b"JPY-OIS", _create_swap_quote_snapshot_message().model_dump_json().encode("utf-8"))
        ]
    )

    subscriber = ZeroMqSwapQuoteSnapshotSubscriber(
        socket=socket,
        topics={"JPY-OIS"},
    )

    stream = subscriber.subscribe()

    try:
        snapshot = await anext(stream)
    finally:
        await stream.aclose()

    assert snapshot.quote_set_id.value == "JPY-TONA"
    assert snapshot.quotes[0].par_rate == Decimal("0.01")



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