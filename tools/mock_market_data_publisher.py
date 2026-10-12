import asyncio
import datetime
from decimal import ROUND_HALF_UP, Decimal
import random

import zmq
import zmq.asyncio

from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import SwapQuoteMessage, SwapQuoteSnapshotMessage, TenorMessage

ENDPOINT = "tcp://127.0.0.1:5555"

async def main() -> None:
    context = zmq.asyncio.Context.instance()

    socket = context.socket(zmq.PUB)
    socket.bind(ENDPOINT)

    print(
        f"[MARKET DATA] ",
        f"publisher started: {ENDPOINT}"
    )

    snapshot = SwapQuoteSnapshotMessage(
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

    try:
        while True:
            await socket.send_multipart(
                [
                    b"JPY-OIS",
                    snapshot.model_dump_json().encode("utf-8"),
                ]
            )

            print(
                "[PUBLISH] \n"
                f"JPY-OIS={snapshot}"
            )

            for quote in snapshot.quotes:
                quote.par_rate = (quote.par_rate + Decimal(random.gauss(0, 0.001))).quantize(Decimal("0.0000001"), rounding=ROUND_HALF_UP)
            snapshot.as_of = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=9)))

            await asyncio.sleep(2)



    finally:
        socket.close(linger=0)


if __name__ == "__main__":
    asyncio.run(main())
    