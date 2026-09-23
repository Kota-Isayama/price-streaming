# application/integration/outbox_mapper.py

from datetime import datetime

from request_for_quote.application.port.outbox_repository import (
    OutboxEvent,
)


def to_outbox_event(
    *,
    event_id: str,
    event_type: str,
    schema_version: int,
    aggregate_type: str,
    aggregate_id: str,
    payload: dict,
    occurred_at: datetime,
    created_at: datetime,
) -> OutboxEvent:
    return OutboxEvent(
        event_id=event_id,
        event_type=event_type,
        schema_version=schema_version,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload=payload,
        occurred_at=occurred_at,
        created_at=created_at,
    )
