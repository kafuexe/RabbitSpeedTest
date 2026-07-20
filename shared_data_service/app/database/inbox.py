"""Inbox table for consumer idempotency.

A CloudEvent id is unique per source; recording (source, event_id) in the SAME
transaction as the module write makes duplicate deliveries no-ops, safely
across many service instances (the dedup state lives in PostgreSQL, not in
process memory).
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    source: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
