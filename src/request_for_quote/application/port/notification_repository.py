import abc
from request_for_quote.application.notification.model import Notification


class INotificationRepository(abc.ABC):
    @abc.abstractmethod
    async def save(self, notification: Notification) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def find_by_source(
        self,
        *,
        source_event_id: str,
        recipient: str,
    ) -> Notification | None:
        raise NotImplementedError
    