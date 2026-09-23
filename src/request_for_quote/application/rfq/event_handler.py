from urllib import request
import uuid

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.integration.outbox_mapper import to_outbox_event
from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.domain.request_for_quote.events import RfqRegistredDomainEvent
from request_for_quote.domain.shared.aware_datetime import AwareDateTime


class RfqRegisteredHandler:
    async def __call__(
        self,
        event: RfqRegistredDomainEvent,
        *,
        uow: IRfqUnitOfWork,
    ) -> None:
        created_at = AwareDateTime.now()

        # Integration Event
        registered = RfqRegisteredIntegrationEvent(
            event_id=str(uuid.uuid4()),
            rfq_id=event.rfq_id,
            revision=event.revision,
            product=event.product,
            assigned_trader=event.assigned_trader,
            registered_by=event.registered_by,
            occurred_at=event.occurred_at.value,
        )

        await uow.get_outbox_repository().add(
            to_outbox_event(
                event_id=registered.event_id,
                event_type=registered.event_type,
                schema_version=registered.schema_version,
                aggregate_type="rfq",
                aggregate_id=registered.rfq_id,
                payload=registered.model_dump(
                    mode="json",
                    exclude={
                        "event_id",
                        "event_type",
                        "schema_version",
                    }
                ),
                occurred_at=registered.occurred_at,
                created_at=created_at.value,
            )
        )
