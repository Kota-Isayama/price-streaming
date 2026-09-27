# application/ports/outbox_repository.py

import abc
from typing import Any, Protocol

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    event_type: str
    schema_version: int

    aggregate_type: str
    aggregate_id: str

    payload: dict[str, Any]

    occurred_at: datetime
    created_at: datetime

class OutboxRepository(abc.ABC):
    @abc.abstractmethod
    async def add(
        self,
        event: OutboxEvent,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def claim_pending(
        self,
        claim_id: str,
        limit: int,
        claim_until: datetime,
    ) -> list[OutboxEvent]:
        raise NotImplementedError

    @abc.abstractmethod
    async def mark_failed(
        self,
        event_id: str,
        claim_id: str,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def mark_published(
        self,
        event_id: str,
        claim_id: str,
        published_at: datetime,
    ) -> None:
        raise NotImplementedError
    