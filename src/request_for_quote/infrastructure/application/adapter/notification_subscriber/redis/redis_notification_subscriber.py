from datetime import datetime
import json
from typing import AsyncGenerator, AsyncIterator

from pydantic import BaseModel
import redis.asyncio as redis

from request_for_quote.application.port.notification_subscriber import INotificationSubscriber, NotificationMessage


class RedisNotificationSubscriber(INotificationSubscriber):
    def __init__(self, redis_client: redis.Redis, channel_prefix: str) -> None:
        self._redis_client = redis_client
        self._channel_prefix = channel_prefix

    async def subscribe(self, recipient: str) -> AsyncIterator[NotificationMessage]:
        channel = f"{self._channel_prefix}:{recipient}"

        pubsub = self._redis_client.pubsub(
            ignore_subscribe_messages=True,
        )

        await pubsub.subscribe(channel)

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                yield NotificationMessage.model_validate_json(message["data"])

        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
