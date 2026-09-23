from typing import Callable
import uuid

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.notification.model import Notification, NotificationType
from request_for_quote.application.port.notification_publisher import INotificationPublisher
from request_for_quote.application.port.notification_unit_of_work import INotificationUnitOfWork


class NotifyAssignedTraderOnRfqRegisteredUseCase:
    def __init__(
        self,
        notification_uow_factory: Callable[[], INotificationUnitOfWork],
        notification_publisher: INotificationPublisher,
    ) -> None:
        self._notification_uow_factory = notification_uow_factory
        self._notification_publisher = notification_publisher

    async def execute(
        self,
        event: RfqRegisteredIntegrationEvent,
    ) -> Notification:
        notification = Notification(
            notification_id=str(uuid.uuid4()),
            recipient=event.assigned_trader,
            notification_type=NotificationType.RFQ_REGISTERED,
            resource_type="rfq",
            resource_id=event.rfq_id,
            source_event_id=event.event_id,
            created_at=event.occurred_at,
            read_at=None,
        )

        async with self._notification_uow_factory() as uow:
            existing = await uow.notifications.find_by_source(
                source_event_id=event.event_id,
                recipient=event.assigned_trader,
            )

            # 冪等性
            if existing is not None:
                return existing

            await uow.notifications.save(
                notification=notification,
            )

            await uow.commit()

        await self._notification_publisher.publish(notification)

        return notification
    