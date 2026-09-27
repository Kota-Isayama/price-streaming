from datetime import datetime, timezone

from sqlalchemy import Select, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from request_for_quote.application.port.outbox_repository import OutboxEvent, OutboxRepository
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.postgres.models import OutboxEventOrm


class SqlAlchemyOutboxRepository(OutboxRepository):
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
                schema_version=event.schema_version,
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                payload=event.payload,
                occurred_at=event.occurred_at,
                created_at=event.created_at,
            )
        )

    # async def list_pending(
    #     self,
    #     limit: int,
    # ) -> list[RfqPricingLifecycleEvent]:
    #         result = await self._session.execute(
    #             select(OutboxEventOrm)
    #             .where(
    #                 OutboxEventOrm.published.is_(False)
    #             )
    #             .order_by(
    #                 OutboxEventOrm.created_at
    #             )
    #             .limit(limit)
    #         )

    #         rows = result.scalars().all()

    #         return [
    #             OutboxEvent(
    #                 event_id=row.event_id,
    #                 event_type=row.event_type,
    #                 payload=row.payload,
    #                 created_at=row.created_at,
    #             )
    #             for row in rows
    #         ]

    async def claim_pending(
        self,
        claim_id: str,
        limit: int,
        claim_until: datetime,
    ) -> list[OutboxEvent]:
        now = AwareDateTime.now()

        stmt = (
            select(OutboxEventOrm)
            .where(
                OutboxEventOrm.published_at.is_(None),
                or_(
                    OutboxEventOrm.claimed_until.is_(None),
                    OutboxEventOrm.claimed_until <= now.value,
                ),
            )
            .order_by(
                OutboxEventOrm.occurred_at
            )
            .limit(limit)
            .with_for_update(
                skip_locked=True,
            )
        )

        result = await self._session.execute(stmt)
        rows = result.scalars().all()

        events = []

        for row in rows:
            row.claim_id = claim_id
            row.claimed_until = claim_until
            row.attempt_count += 1

            events.append(
                OutboxEvent(
                    event_id=row.event_id,
                    event_type=row.event_type,
                    schema_version=row.schema_version,
                    aggregate_type=row.aggregate_type,
                    aggregate_id=row.aggregate_id,
                    payload=row.payload,
                    occurred_at=row.occurred_at,
                    created_at=row.created_at,
                )
            )

        return events

    async def mark_published(
        self,
        event_id: str,
        claim_id: str,
        published_at: datetime,
    ) -> None:
        stmt = (
            update(OutboxEventOrm)
            .where(
                OutboxEventOrm.event_id==event_id,
                OutboxEventOrm.claim_id==claim_id,
                OutboxEventOrm.published_at.is_(None),
            )
            .values(
                published_at=published_at,
                claim_id=None,
                claimed_until=None,
            )
        )

        await self._session.execute(stmt)

    async def mark_failed(
        self,
        event_id: str,
        claim_id: str,
    ) -> None:
        stmt = (
            update(OutboxEventOrm)
            .where(
                OutboxEventOrm.event_id == event_id,
                OutboxEventOrm.claim_id == claim_id,
            )
            .values(
                claim_id=None,
                claimed_until=None,
            )
        )

        await self._session.execute(stmt)
