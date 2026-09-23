import abc
from datetime import datetime
from typing import AsyncIterator

from pydantic import BaseModel

from request_for_quote.application.notification.model import NotificationType


class NotificationMessage(BaseModel):
    notification_id: str
    notification_type: NotificationType
    resource_type: str
    resource_id: str
    created_at: datetime
    

class INotificationSubscriber(abc.ABC):
    @abc.abstractmethod
    async def subscribe(self, recipient: str) -> AsyncIterator[NotificationMessage]:
        raise NotImplementedError
    