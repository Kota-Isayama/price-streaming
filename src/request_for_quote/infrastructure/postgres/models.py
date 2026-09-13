# infrastructure/database/models.py

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String
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
    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
