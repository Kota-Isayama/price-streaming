from dataclasses import dataclass
from datetime import timedelta
from typing import Callable
import uuid

from request_for_quote.application.port.integration_event_publisher import IIntegrationEventPublisher
from request_for_quote.application.port.outbox_delivery_uow import OutboxDeliveryUnitOfWork
from request_for_quote.domain.shared.aware_datetime import AwareDateTime


@dataclass(frozen=True)
class PublishOutboxEventsResult:
    claimed: int
    published: int
    failed: int


class PublishOutboxEventsUseCase:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], OutboxDeliveryUnitOfWork],
        publisher: IIntegrationEventPublisher,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher

    async def execute(self, *, limit: int) -> PublishOutboxEventsResult:
        claim_id = str(uuid.uuid4())
        now = AwareDateTime.now()

        # 1. Outboxをclaim
        async with self._uow_factory() as uow:
            events = await uow.outbox.claim_pending(
                claim_id=claim_id,
                limit=limit,
                claim_until=now.value + timedelta(seconds=30),
            )

            await uow.commit()

        published = 0
        failed = 0

        # 2. Brokerへpublish
        for event in events:
            try:
                await self._publisher.publish(event)
            except Exception as e:
                failed += 1

                async with self._uow_factory() as uow:
                    await uow.outbox.mark_failed(
                        event_id=event.event_id,
                        claim_id=claim_id,
                    )

                    await uow.commit()

                print(
                    f"[OUTBOX] "
                    f"Failed to publish event {event.event_id} (aggregate type: {event.aggregate_type} aggregate id: {event.aggregate_type} event type {event.event_type})"
                )
                continue

            # 3. publishedにする
            async with self._uow_factory() as uow:
                await uow.outbox.mark_published(
                    event_id=event.event_id,
                    claim_id=claim_id,
                    published_at=AwareDateTime.now().value,
                )

                await uow.commit()
            print(
                "[OUTBOX] "
                f"Published event {event.event_id} (aggregate type: {event.aggregate_type} aggregate id: {event.aggregate_type} event type {event.event_type})"
            )
            published += 1

        return PublishOutboxEventsResult(
            claimed=len(events),
            published=published,
            failed=failed,
        )
    