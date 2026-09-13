# application/ports/outbox_repository.py

import abc
from typing import Protocol

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    event_type: str
    payload: dict
    created_at: datetime

class OutboxRepository(abc.ABC):
    @abc.abstractmethod
    async def add(
        self,
        event: OutboxEvent,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def list_pending(
        self,
        limit: int,
    ) -> list[OutboxEvent]:
        raise NotImplementedError

    @abc.abstractmethod
    async def mark_published(
        self,
        event_id: str,
    ) -> None:
        raise NotImplementedError
    