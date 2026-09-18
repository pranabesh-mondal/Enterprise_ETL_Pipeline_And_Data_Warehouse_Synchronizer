"""Base Pydantic schemas shared by all API data models.

Day 1-2 (Week 1): data models defined with Pydantic v2. Every source
model inherits `BaseSchema`; extraction results are wrapped in
`ExtractedRecord` envelopes for the raw S3 landing zone.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Source(str, Enum):
    STRIPE = "stripe"
    SALESFORCE = "salesforce"


class BaseSchema(BaseModel):
    """Common config for all source data models."""

    model_config = ConfigDict(
        extra="ignore",          # ignore unknown API fields
        populate_by_name=True,   # allow field-name or alias population
        str_strip_whitespace=True,
    )


class ExtractedRecord(BaseSchema):
    """Envelope for one validated record landing in the raw data lake."""

    source: Source
    resource: str
    external_id: str
    extracted_at: datetime
    data: dict[str, Any]


class ValidationErrorRecord(BaseSchema):
    """A quarantined record that failed Pydantic validation."""

    source: Source
    resource: str
    index: int
    payload: dict[str, Any]
    errors: list[dict[str, Any]]


class ExtractionResult(BaseSchema):
    """Outcome of extracting + validating one resource."""

    source: Source
    resource: str
    total_fetched: int
    valid_records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    @property
    def valid_count(self) -> int:
        return len(self.valid_records)

    @property
    def error_count(self) -> int:
        return len(self.errors)


def validate_batch(
    model_cls: type[BaseModel],
    items: list[dict[str, Any]],
    source: Source,
    resource: str,
) -> ExtractionResult:
    """Validate raw API payloads against a Pydantic model.

    Invalid records are quarantined into `errors` (with full Pydantic error
    details) instead of failing the whole batch - per-batch error isolation.
    """
    valid_records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    extracted_at = utcnow()

    for index, item in enumerate(items):
        try:
            record = model_cls.model_validate(item)
            envelope = ExtractedRecord(
                source=source,
                resource=resource,
                external_id=str(getattr(record, "id", "")),
                extracted_at=extracted_at,
                data=record.model_dump(mode="json"),
            )
            valid_records.append(envelope.model_dump(mode="json"))
        except ValidationError as exc:
            errors.append(
                ValidationErrorRecord(
                    source=source,
                    resource=resource,
                    index=index,
                    payload=item,
                    errors=exc.errors(include_url=False),
                ).model_dump(mode="json")
            )

    return ExtractionResult(
        source=source,
        resource=resource,
        total_fetched=len(items),
        valid_records=valid_records,
        errors=errors,
    )
