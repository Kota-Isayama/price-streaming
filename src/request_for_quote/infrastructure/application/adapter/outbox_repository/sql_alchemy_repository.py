from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.application.pricing.lifecycle import RfqPricingLifecycleEvent
from request_for_quote.infrastructure.postgres.models import OutboxEventOrm


class SqlAlchemyOutboxRepository:
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self._session = session

    async def add(
        self,
        event: OutboxEvent,
    ) -> None:
        self._session.add(
            OutboxEventOrm(
                event_id=event.event_id,
                event_type=event.event_type,
                payload=event.payload,
                created_at=event.created_at,
            )
        )

    async def list_pending(
        self,
        limit: int,
    ) -> list[RfqPricingLifecycleEvent]:
            result = await self._session.execute(
                select(OutboxEventOrm)
                .where(
                    OutboxEventOrm.published.is_(False)
                )
                .order_by(
                    OutboxEventOrm.created_at
                )
                .limit(limit)
            )

            rows = result.scalars().all()

            return [
                OutboxEvent(
                    event_id=row.event_id,
                    event_type=row.event_type,
                    payload=row.payload,
                    created_at=row.created_at,
                )
                for row in rows
            ]

    async def mark_published(
        self,
        event_id: str,
    ) -> None:
        await self._session.execute(
            update(OutboxEventOrm)
            .where(
                OutboxEventOrm.event_id==event_id,
            )
            .values(
                published=True,
                published_at=datetime.now(timezone.utc),
            )
        )
