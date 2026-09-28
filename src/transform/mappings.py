"""Declarative source -> unified schema mappings - Week 2, Day 4-6.

Each `ResourceMapping` describes how one source resource's flat API fields
become unified warehouse columns. Adding a new source/resource is a data
change here, not a code change in the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.models.base import Source
from src.models.unified import EntityType, RecordStatus
from src.transform.clean import (
    clean_string,
    major_to_minor,
    minor_to_major,
    standardize_currency,
    standardize_datetime,
)


class MappingError(Exception):
    """Raised when a raw record cannot be mapped to the unified schema."""


@dataclass(frozen=True)
class ResourceMapping:
    """Field-level mapping for one (source, resource) pair."""

    source: Source
    resource: str
    entity_type: EntityType
    id_field: str
    created_field: str
    updated_field: str | None = None
    status_field: str | None = None
    currency_field: str | None = None
    amount_field: str | None = None
    amount_is_major: bool = False
    default_currency: str | None = None
    default_status: RecordStatus = RecordStatus.UNKNOWN
    field_map: dict[str, str] = field(default_factory=dict)
    attribute_map: dict[str, str] = field(default_factory=dict)
    status_map: dict[str, RecordStatus] = field(default_factory=dict)
    title_case_fields: tuple[str, ...] = ()


def _pick(record: dict[str, Any], path: str) -> Any:
    """Read a (possibly dotted) field path from a raw record."""
    current: Any = record
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def resolve_status(raw_status: Any, mapping: ResourceMapping) -> RecordStatus:
    """Map a source status onto the normalized status enum."""
    text = clean_string(raw_status)
    if text is None:
        return mapping.default_status
    return mapping.status_map.get(text, mapping.status_map.get(text.lower(), RecordStatus.UNKNOWN))


def map_record(
    record: dict[str, Any],
    mapping: ResourceMapping,
    *,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Map one raw API payload into unified-schema field values."""
    if not isinstance(record, dict):
        raise MappingError("record is not a JSON object")

    source_id = clean_string(_pick(record, mapping.id_field))
    if not source_id:
        raise MappingError(f"missing id field '{mapping.id_field}'")

    row: dict[str, Any] = {
        "source": mapping.source.value,
        "source_resource": mapping.resource,
        "source_id": source_id,
        "entity_type": mapping.entity_type.value,
        "created_at": standardize_datetime(_pick(record, mapping.created_field)),
        "updated_at": (
            standardize_datetime(_pick(record, mapping.updated_field))
            if mapping.updated_field
            else None
        ),
        "ingested_at": ingested_at,
        "status": resolve_status(
            _pick(record, mapping.status_field) if mapping.status_field else None,
            mapping,
        ).value,
    }

    if row["created_at"] is None:
        raise MappingError(f"missing or unparseable '{mapping.created_field}'")

    for target, source_field in mapping.field_map.items():
        value = clean_string(
            _pick(record, source_field),
            title_case=target in mapping.title_case_fields,
        )
        if target == "email" and value is not None:
            value = value.lower()
        row[target] = value

    currency = (
        standardize_currency(_pick(record, mapping.currency_field))
        if mapping.currency_field
        else mapping.default_currency
    )
    currency = currency or mapping.default_currency
    row["currency"] = currency

    raw_amount = _pick(record, mapping.amount_field) if mapping.amount_field else None
    if mapping.amount_is_major:
        row["amount_minor"] = major_to_minor(raw_amount, currency)
        row["amount"] = minor_to_major(row["amount_minor"], currency)
    else:
        row["amount_minor"] = int(raw_amount) if raw_amount is not None else None
        row["amount"] = minor_to_major(raw_amount, currency)

    attributes: dict[str, Any] = {}
    for target, source_field in mapping.attribute_map.items():
        value = clean_string(_pick(record, source_field))
        if value is not None:
            attributes[target] = value
    if mapping.status_field:
        raw_status = clean_string(_pick(record, mapping.status_field))
        if raw_status is not None:
            attributes["raw_status"] = raw_status
    row["attributes"] = attributes
    return row


MAPPINGS: dict[tuple[Source, str], ResourceMapping] = {
    (Source.STRIPE, "customers"): ResourceMapping(
        source=Source.STRIPE,
        resource="customers",
        entity_type=EntityType.CUSTOMER,
        id_field="id",
        created_field="created",
        field_map={"name": "name", "email": "email"},
        attribute_map={"description": "description", "livemode": "livemode"},
        default_status=RecordStatus.ACTIVE,
        title_case_fields=("name",),
    ),
    (Source.STRIPE, "charges"): ResourceMapping(
        source=Source.STRIPE,
        resource="charges",
        entity_type=EntityType.TRANSACTION,
        id_field="id",
        created_field="created",
        status_field="status",
        currency_field="currency",
        amount_field="amount",  # Stripe amounts are minor units (cents)
        field_map={"name": "description"},
        attribute_map={"customer": "customer", "description": "description"},
        status_map={
            "succeeded": RecordStatus.SUCCEEDED,
            "pending": RecordStatus.PENDING,
            "failed": RecordStatus.FAILED,
        },
    ),
    (Source.STRIPE, "invoices"): ResourceMapping(
        source=Source.STRIPE,
        resource="invoices",
        entity_type=EntityType.TRANSACTION,
        id_field="id",
        created_field="created",
        status_field="status",
        currency_field="currency",
        amount_field="amount_due",
        attribute_map={"customer": "customer", "amount_paid": "amount_paid"},
        status_map={
            "paid": RecordStatus.PAID,
            "open": RecordStatus.OPEN,
            "void": RecordStatus.CANCELED,
            "uncollectible": RecordStatus.FAILED,
            "draft": RecordStatus.PENDING,
        },
    ),
    (Source.SALESFORCE, "accounts"): ResourceMapping(
        source=Source.SALESFORCE,
        resource="accounts",
        entity_type=EntityType.CUSTOMER,
        id_field="Id",
        created_field="CreatedDate",
        updated_field="LastModifiedDate",
        field_map={"name": "Name"},
        attribute_map={"industry": "Industry", "website": "Website"},
        default_status=RecordStatus.ACTIVE,
        title_case_fields=("name",),
    ),
    (Source.SALESFORCE, "opportunities"): ResourceMapping(
        source=Source.SALESFORCE,
        resource="opportunities",
        entity_type=EntityType.OPPORTUNITY,
        id_field="Id",
        created_field="CreatedDate",
        updated_field="LastModifiedDate",
        status_field="StageName",
        amount_field="Amount",
        amount_is_major=True,  # Salesforce Amount is a major-unit currency value
        field_map={"name": "Name"},
        attribute_map={"close_date": "CloseDate", "stage_name": "StageName"},
        status_map={
            "Prospecting": RecordStatus.OPEN,
            "Qualification": RecordStatus.OPEN,
            "Needs Analysis": RecordStatus.OPEN,
            "Proposal/Price Quote": RecordStatus.OPEN,
            "Negotiation/Review": RecordStatus.OPEN,
            "Closed Won": RecordStatus.CLOSED_WON,
            "Closed Lost": RecordStatus.CLOSED_LOST,
        },
        title_case_fields=("name",),
    ),
}


def get_mapping(source: Source, resource: str) -> ResourceMapping:
    """Look up the mapping for a source/resource pair."""
    try:
        return MAPPINGS[(source, resource)]
    except KeyError as exc:
        known = sorted(f"{src.value}/{res}" for src, res in MAPPINGS)
        raise ValueError(
            f"No unified mapping for {source.value}/{resource}. Known: {known}"
        ) from exc
