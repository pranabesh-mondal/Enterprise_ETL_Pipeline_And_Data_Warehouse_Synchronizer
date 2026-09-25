"""Unified (canonical) warehouse schema - Week 2, Day 4-6.

Stripe and Salesforce records are mapped into these models so downstream
consumers receive one consistent shape regardless of the origin system.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from .base import BaseSchema, Source

CURRENCY_PATTERN = r"^[A-Z]{3}$"


class EntityType(StrEnum):
    """Canonical entity families in the warehouse."""

    CUSTOMER = "customer"
    TRANSACTION = "transaction"
    OPPORTUNITY = "opportunity"


class RecordStatus(StrEnum):
    """Normalized lifecycle status across all sources."""

    ACTIVE = "active"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUNDED = "refunded"
    CANCELED = "canceled"
    PAID = "paid"
    OPEN = "open"
    CLOSED_WON = "closed_won"
    CLOSED_LOST = "closed_lost"
    UNKNOWN = "unknown"


class UnifiedRecord(BaseSchema):
    """Base canonical record: lineage + shared dimensions."""

    # ---- lineage / keys ----
    source: Source
    source_resource: str
    source_id: str
    entity_type: EntityType

    # ---- shared dimensions ----
    name: str | None = None
    email: str | None = None
    currency: str | None = Field(default=None, pattern=CURRENCY_PATTERN)
    amount_minor: int | None = Field(default=None, ge=0)
    amount: Decimal | None = None
    status: RecordStatus = RecordStatus.UNKNOWN

    # ---- standard timestamps (always UTC) ----
    created_at: datetime
    updated_at: datetime | None = None
    ingested_at: datetime

    # ---- source-specific extras (kept, never dropped silently) ----
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency", mode="before")
    @classmethod
    def _normalize_currency(cls, value: Any) -> Any:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value

    @property
    def unified_id(self) -> str:
        """Stable cross-source surrogate key: source:entity_type:source_id."""
        return f"{self.source.value}:{self.entity_type.value}:{self.source_id}"


class UnifiedCustomer(UnifiedRecord):
    """Companies/people (Stripe customers, Salesforce accounts)."""

    entity_type: EntityType = EntityType.CUSTOMER


class UnifiedTransaction(UnifiedRecord):
    """Money movements (Stripe charges, Stripe invoices)."""

    entity_type: EntityType = EntityType.TRANSACTION
    currency: str = Field(pattern=CURRENCY_PATTERN)
    amount_minor: int = Field(ge=0)


class UnifiedOpportunity(UnifiedRecord):
    """Sales pipeline (Salesforce opportunities)."""

    entity_type: EntityType = EntityType.OPPORTUNITY


ENTITY_MODELS: dict[EntityType, type[UnifiedRecord]] = {
    EntityType.CUSTOMER: UnifiedCustomer,
    EntityType.TRANSACTION: UnifiedTransaction,
    EntityType.OPPORTUNITY: UnifiedOpportunity,
}

# Canonical column order used when materializing DataFrames / warehouse tables.
UNIFIED_COLUMNS: list[str] = [
    "unified_id",
    "source",
    "source_resource",
    "source_id",
    "entity_type",
    "name",
    "email",
    "currency",
    "amount_minor",
    "amount",
    "status",
    "created_at",
    "updated_at",
    "ingested_at",
    "attributes",
]
