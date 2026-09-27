import json

import redis.asyncio as redis

from request_for_quote.application.port.pricing_update_publisher import IPricingUpdatePublisher


class RedisPricingUpdatePublisher(IPricingUpdatePublisher):
    def __init__(self, redis: redis.Redis, channel_prefix: str):
        self._redis = redis
        self._channel_prefix = channel_prefix

    async def publish(self, request_id, price):
        channel = self._channel(request_id)

        payload = json.dumps(
            {
                "request_id": request_id,
                "par_rate": str(price.par_rate),
            }
        )

        await self._redis.publish(
            channel,
            payload,
        )

    def _channel(self, request_id: str) -> str:
        return f"{self._channel_prefix}:{request_id}"
    