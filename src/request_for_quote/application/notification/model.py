import dataclasses
import datetime
import enum


class NotificationType(enum.StrEnum):
    RFQ_REGISTERED = "rfq_registered"
    RFQ_QUOTE_SUBMITTED = "rfq_quote_submitted"


@dataclasses.dataclass(frozen=True)
class Notification:
    notification_id: str
    recipient: str
    notification_type: NotificationType

    resource_type: str
    resource_id: str

    source_event_id: str

    created_at: datetime.datetime
    read_at: datetime.datetime | None
