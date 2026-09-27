import datetime
from typing import Any

from pydantic import BaseModel


class IntegrationEventEnvelope(BaseModel):
    message_id: str
    message_type: str
    schema_version: int
    payload: dict[str, Any]
    occurred_at: datetime.datetime
    