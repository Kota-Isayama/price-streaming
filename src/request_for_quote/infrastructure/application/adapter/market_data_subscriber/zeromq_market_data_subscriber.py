import json
from typing import AsyncIterator

import zmq
import zmq.asyncio

from request_for_quote.application.port.market_data_subscriber import IMarketDataSubscriber, MarketDataUpdate
from request_for_quote.domain.market.market import MarketDataId, MarketDataValue


class ZeroMqMarketDataSubscriber(IMarketDataSubscriber):
    def __init__(
        self,
        endpoint: str,
        topics: set[str],
    ) -> None:
        context = zmq.asyncio.Context.instance()

        self._socket = context.socket(zmq.SUB)  # ZeroMQの場合、HTTP requestやSQL Alchemyのように、コネクションを使い回したりする必要はないのか？
        self._socket.connect(endpoint)

        for topic in topics:
            self._socket.setsockopt_string(  # setsocket_stringとは？
                zmq.SUBSCRIBE,
                topic,
            )

    async def subscribe(self) -> AsyncIterator[MarketDataUpdate]:
        while True:
            topic_bytes, payload_bytes = await self._socket.recv_multipart()  # What is multipart??

            topic = topic_bytes.decode("utf-8")
            payload = json.loads(payload_bytes.decode("utf-8"))  # 型をつけたい

            yield MarketDataUpdate(
                market_data_id=MarketDataId(topic),
                value=MarketDataValue(payload["value"])
            )


    def close(self) -> None:
        self._socket.close(linger=0)  # 何をしている？