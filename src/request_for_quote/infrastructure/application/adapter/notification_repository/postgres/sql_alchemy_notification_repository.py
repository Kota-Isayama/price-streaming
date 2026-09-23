from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.application.notification.model import Notification, NotificationType
from request_for_quote.application.port.notification_repository import INotificationRepository
from request_for_quote.infrastructure.postgres.models import NotificationOrm


class SqlAlchemyNotificationRepository(INotificationRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(
        self,
        notification: Notification,
    ) -> None:
        orm = NotificationOrm(
            notification_id=notification.notification_id,
            recipient=notification.recipient,
            notification_type=notification.notification_type.value,
            resource_type=notification.resource_type,
            resource_id=notification.resource_id,
            source_event_id=notification.source_event_id,
            created_at=notification.created_at,
            read_at=notification.read_at,
        )

        self._session.add(orm)

    async def find_by_source(
        self,
        *,
        source_event_id: str,
        recipient: str,
    ) -> Notification | None:
        stmt = (
            select(NotificationOrm)
            .where(
                NotificationOrm.source_event_id == source_event_id,
                NotificationOrm.recipient == recipient,
            )
        )

        result = await self._session.execute(stmt)

        orm = result.scalar_one_or_none()

        if orm is None:
            return None

        return Notification(
            notification_id=orm.notification_id,
            recipient=orm.recipient,
            notification_type=NotificationType(
                orm.notification_type
            ),
            resource_type=orm.resource_type,
            resource_id=orm.resource_id,
            source_event_id=orm.source_event_id,
            created_at=orm.created_at,
            read_at=orm.read_at,
        )
    