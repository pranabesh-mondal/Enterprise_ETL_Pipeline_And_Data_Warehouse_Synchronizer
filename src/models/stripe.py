"""Pydantic models for Stripe API resources (Day 1-2, Week 1)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import AliasChoices, Field, field_validator

from .base import BaseSchema


def epoch_to_datetime(value: Any) -> Any:
    """Stripe returns epoch seconds for timestamps - normalize to UTC datetime."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return value


class StripeTimestampMixin(BaseSchema):
    """Applies epoch -> datetime conversion to the `created` field."""

    @field_validator("created", mode="before", check_fields=False)
    @classmethod
    def _convert_epoch(cls, value: Any) -> Any:
        return epoch_to_datetime(value)


class StripeCustomer(StripeTimestampMixin):
    id: str
    email: str | None = None
    name: str | None = None
    description: str | None = None
    created: datetime
    livemode: bool = False


class StripeCharge(StripeTimestampMixin):
    id: str
    amount: int = Field(ge=0)  # in cents
    currency: str
    status: str
    customer: str | None = None
    description: str | None = None
    created: datetime
    livemode: bool = False

    @field_validator("currency")
    @classmethod
    def _lowercase_currency(cls, value: str) -> str:
        return value.lower()


class StripeInvoice(StripeTimestampMixin):
    id: str
    customer: str | None = None
    amount_due: int
    amount_paid: int
    currency: str
    status: str | None = None
    created: datetime

    @field_validator("currency")
    @classmethod
    def _lowercase_currency(cls, value: str) -> str:
        return value.lower()


# resource name (Stripe API path) -> validating model
STRIPE_RESOURCE_MODELS: dict[str, type[BaseSchema]] = {
    "customers": StripeCustomer,
    "charges": StripeCharge,
    "invoices": StripeInvoice,
}
