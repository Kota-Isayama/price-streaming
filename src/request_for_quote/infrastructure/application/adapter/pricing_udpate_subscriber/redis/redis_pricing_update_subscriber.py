import asyncio
from decimal import Decimal
import json
from typing import AsyncIterator

from redis.asyncio.client import PubSub

from request_for_quote.application.port.pricing_update_subscriber import IPricingUpdateSubscriber, PricingUpdate


class RedisPricingUpdateSubscriber(IPricingUpdateSubscriber):
    def __init__(self, pubsub: PubSub):
        self._pubsub = pubsub
        self._request_ids: set[str] = set()
        self._has_subscription = asyncio.Event()

    async def set_request_ids(self, request_ids) -> None:
        added = request_ids - self._request_ids
        removed = self._request_ids - request_ids

        if added:
            await self._pubsub.subscribe(
                *[
                    self._channel(request_id)
                    for request_id in added
                ]
            )

        if removed:
            await self._pubsub.unsubscribe(
                *[
                    self._channel(request_id)
                    for request_id in removed
                ]
            )

        self._request_ids = request_ids

        if self._request_ids:
            self._has_subscription.set()
        else:
            self._has_subscription.clear()

    async def updates(
        self,
    ) -> AsyncIterator[PricingUpdate]:
        while True:
            # まだ１件もsubscribeしていなければ
            # set_request_ids()されるまでここで待つ
            await self._has_subscription.wait()

            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    continue

                yield self._deserialize(json.loads(message["data"].decode("utf-8")))

    async def close(self) -> None:
        if self._request_ids:
            await self._pubsub.unsubscribe(
                *[
                    self._channel(request_id)
                    for request_id in self._request_ids
                ]
            )

        await self._pubsub.aclose()

    @classmethod
    def _channel(cls, request_id: str) -> str:
        return f"pricing:{request_id}"

    @classmethod
    def _deserialize(cls, data: dict) -> PricingUpdate:
        return PricingUpdate(
            request_id=data["request_id"],
            par_rate=Decimal(data["par_rate"]),
        )
    