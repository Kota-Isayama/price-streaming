import datetime
import enum
from typing import Literal

from pydantic import BaseModel

from request_for_quote.domain.product.swap import InterestRateSwap


class EventType(enum.StrEnum):
    RFQ_REGISTERED = "rfq.registered"


class RfqRegisteredIntegrationEvent(BaseModel):
    event_id: str

    event_type: Literal[EventType.RFQ_REGISTERED] = EventType.RFQ_REGISTERED
    schema_version: int = 1

    rfq_id: str
    revision: int
    product: InterestRateSwap
    assigned_trader: str
    registered_by: str

    occurred_at: datetime.datetime
