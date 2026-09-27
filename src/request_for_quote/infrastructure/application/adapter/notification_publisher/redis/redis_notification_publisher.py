import json

import redis.asyncio as redis

from request_for_quote.application.notification.model import Notification
from request_for_quote.application.port.notification_publisher import INotificationPublisher


class RedisNotificationPublisher(INotificationPublisher):
    def __init__(
        self,
        redis_client: redis.Redis,
        channel_prefix: str,
    ) -> None:
        self._redis_client = redis_client
        self._channel_prefix = channel_prefix

    async def publish(self, notification: Notification) -> None:
        channel = f"{self._channel_prefix}:{notification.recipient}"

        payload = {
            "notification_id": notification.notification_id,
            "notification_type": notification.notification_type.value,
            "resource_type": notification.resource_type,
            "resource_id": notification.resource_id,
            "created_at": notification.created_at.isoformat(),
        }

        await self._redis_client.publish(
            channel,
            json.dumps(payload),
        )
