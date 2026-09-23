import abc

from request_for_quote.application.notification.model import Notification


class INotificationPublisher(abc.ABC):
    @abc.abstractmethod
    async def publish(self, notification: Notification) -> None:
        raise NotImplementedError
    