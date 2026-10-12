import json
from typing import AsyncGenerator, AsyncIterator

import zmq
import zmq.asyncio

from request_for_quote.application.market.port.swap_quote_snapshot_subscriber import ISwapQuoteSnapshotSubscriber
from request_for_quote.domain.market.swap_quote import SwapQuoteSnapshot
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_mapper import SwapQuoteSnapshotMapper
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import SwapQuoteSnapshotMessage


class ZeroMqSwapQuoteSnapshotSubscriber(ISwapQuoteSnapshotSubscriber):
    def __init__(
        self,
        endpoint: str,
        topics: set[str],
    ) -> None:
        context = zmq.asyncio.Context.instance()

        self._socket = context.socket(zmq.SUB)
        self._socket.connect(endpoint)

        for topic in topics:
            self._socket.setsockopt_string(
                zmq.SUBSCRIBE,
                topic
            )

    async def subscribe(self) -> AsyncGenerator[SwapQuoteSnapshot]:
        while True:
            _, payload_bytes = await self._socket.recv_multipart()

            try:
                message = SwapQuoteSnapshotMessage.model_validate_json(
                    payload_bytes.decode("utf-8"),
                )
            except Exception:
                msg = f"[MARKET] Unsupported Message format: {payload_bytes}"
                print(msg)
                return

            yield SwapQuoteSnapshotMapper.to_domain(message)

    def close(self) -> None:
        self._socket.close(linger=0)
