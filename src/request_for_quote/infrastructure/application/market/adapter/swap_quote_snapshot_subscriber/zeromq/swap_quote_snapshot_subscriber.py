import json
from typing import AsyncGenerator, AsyncIterator

from pydantic import ValidationError
import zmq
import zmq.asyncio

from request_for_quote.application.market.port.swap_quote_snapshot_subscriber import ISwapQuoteSnapshotSubscriber
from request_for_quote.domain.market.swap_quote import SwapQuoteSnapshot
from request_for_quote.domain.shared.error import DomainError
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_mapper import SwapQuoteSnapshotMapper
from request_for_quote.infrastructure.zeromq.market.message.swap_quote_snapshot_message import SwapQuoteSnapshotMessage


class ZeroMqSwapQuoteSnapshotSubscriber(ISwapQuoteSnapshotSubscriber):
    def __init__(
        self,
        socket: zmq.asyncio.Socket,
        topics: set[str],
    ) -> None:
        self._socket = socket

        for topic in topics:
            self._socket.setsockopt_string(
                zmq.SUBSCRIBE,
                topic
            )

    async def subscribe(self) -> AsyncGenerator[SwapQuoteSnapshot]:
        while True:
            _, payload_bytes = await self._socket.recv_multipart()

            try:
                payload = payload_bytes.decode("utf-8")
            except UnicodeDecodeError:
                msg = f"[MARKET] Cannot docode by UTF-8: {payload_bytes}"
                print(msg)
                continue

            try:
                message = SwapQuoteSnapshotMessage.model_validate_json(payload)
            except ValidationError:
                msg = f"[MARKET] Unsupported Message format: {payload_bytes}"
                print(msg)
                continue

            try:
                snapshot = SwapQuoteSnapshotMapper.to_domain(message)
            except DomainError as e:
                msg = f"[MARKET] Cannot map message to domain model: {message=}\n {e}"
                print(msg)
                continue
            
            yield snapshot

    def close(self) -> None:
        self._socket.close(linger=0)
