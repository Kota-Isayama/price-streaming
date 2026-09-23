# infrastructure/database/models.py

from datetime import datetime
from re import I

from sqlalchemy import ARRAY, Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RfqOrm(Base):
    __tablename__ = "rfqs"

    rfq_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )
    revision: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    product_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )
    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )
    assigned_trader: Mapped[str]
    registered_by: Mapped[str]


class PricingRequestOrm(Base):
    __tablename__ = "pricing_requests"

    request_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )
    revision: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
    )
    product_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )
    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )


class OutboxEventOrm(Base):
    __tablename__ = "outbox_event"

    event_id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )
    event_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )
    schema_version: Mapped[int] 
    aggregate_type: Mapped[str]
    aggregate_id: Mapped[str]
    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
    claim_id: Mapped[str] = mapped_column(String, nullable=True)
    claimed_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PricingSessionOrm(Base):
    __tablename__ = "pricing_sessions"

    request_id: Mapped[str] = mapped_column(primary_key=True)
    request: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(nullable=False)
    dependencies: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)


class NotificationOrm(Base):
    __tablename__ = "notifications"

    __table_args__ = (
        UniqueConstraint(
            "source_event_id",
            "recipient",
            "notification_type",
            name="uq_notification_source_recipient_type",
        ),
    )

    notification_id: Mapped[str] = mapped_column(String, primary_key=True)
    recipient: Mapped[str] = mapped_column(String, nullable=False, index=True)
    notification_type: Mapped[str] = mapped_column(String, nullable=False)
    notification_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )
    resource_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    resource_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
        index=True,
    )

    source_event_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    read_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
