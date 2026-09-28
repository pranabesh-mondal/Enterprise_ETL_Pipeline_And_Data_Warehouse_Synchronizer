"""Transform pipeline - Week 2, Day 4-6.

raw payload -> clean/standardize -> map to unified schema -> Pydantic
validate -> deduplicate -> unified records ready for the warehouse.

Records that cannot be mapped or validated are rejected with a reason
instead of failing the whole batch (same isolation principle as extraction).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from src.models.base import Source
from src.models.unified import ENTITY_MODELS, UnifiedRecord
from src.transform.clean_frames import (
    build_dataframe,
    build_frame,
    clean_frame,
    clean_frame_pandas,
    deduplicate_frame,
    deduplicate_frame_pandas,
)
from src.transform.mappings import MappingError, ResourceMapping, get_mapping, map_record
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

ENGINES = ("polars", "pandas")

# Columns the engine-level cleaners normalize.
STRING_COLUMNS = ("name", "email", "currency")
DATETIME_COLUMNS = ("created_at", "updated_at", "ingested_at")
NUMERIC_COLUMNS = ("amount_minor",)


@dataclass
class TransformResult:
    """Outcome of transforming one source resource into unified records."""

    source: Source
    resource: str
    records: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    engine: str = "polars"

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


def _unwrap(raw: Any) -> Any:
    """Accept either a Week-1 envelope ({"data": {...}}) or a bare payload."""
    if isinstance(raw, dict) and "data" in raw and isinstance(raw["data"], dict):
        return raw["data"]
    return raw


def transform_records(
    raw_records: list[Any],
    source: Source,
    resource: str,
    *,
    ingested_at: datetime | None = None,
    engine: str = "polars",
    mapping: ResourceMapping | None = None,
) -> TransformResult:
    """Transform raw payloads for one source resource into unified records."""
    if engine not in ENGINES:
        raise ValueError(f"Unknown engine '{engine}'. Supported: {ENGINES}")

    mapping = mapping or get_mapping(source, resource)
    ingested_at = ingested_at or datetime.now(UTC)
    model = ENTITY_MODELS[mapping.entity_type]
    stats = {
        "input": len(raw_records),
        "mapped": 0,
        "rejected": 0,
        "duplicates_removed": 0,
    }
    rows: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for index, raw in enumerate(raw_records):
        try:
            row = map_record(_unwrap(raw), mapping, ingested_at=ingested_at)
        except MappingError as exc:
            rejected.append(
                {"index": index, "stage": "mapping", "reason": str(exc),
                 "payload": raw if isinstance(raw, dict) else repr(raw)}
            )
            continue

        try:
            validated: UnifiedRecord = model.model_validate(row)
        except ValidationError as exc:
            rejected.append(
                {"index": index, "stage": "validation",
                 "reason": exc.errors(include_url=False)[0].get("msg"),
                 "errors": exc.errors(include_url=False), "payload": row}
            )
            continue

        final = validated.model_dump(mode="json")
        final["unified_id"] = validated.unified_id
        rows.append(final)

    stats["mapped"] = len(rows)
    stats["rejected"] = len(rejected)

    if not rows:
        logger.warning(
            "Transform %s/%s produced 0 unified records (%d rejected)",
            source.value, resource, len(rejected),
        )
        return TransformResult(
            source=source, resource=resource, records=[], rejected=rejected,
            stats=stats, engine=engine,
        )

    records, duplicates = _deduplicate(rows, engine)
    stats["duplicates_removed"] = duplicates
    stats["output"] = len(records)

    logger.info(
        "Transformed %s/%s -> entity=%s in=%d out=%d rejected=%d duplicates=%d (%s)",
        source.value, resource, mapping.entity_type.value, stats["input"],
        len(records), stats["rejected"], duplicates, engine,
    )
    return TransformResult(
        source=source, resource=resource, records=records, rejected=rejected,
        stats=stats, engine=engine,
    )


def _finalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize one output record for the warehouse.

    * drops null values (top-level and inside `attributes`) - SQL-friendly
    * renders datetimes as ISO-8601 with a 'T' separator
    """
    finalized: dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            finalized[key] = value.isoformat()
        elif isinstance(value, dict):
            finalized[key] = {
                nested_key: nested_value
                for nested_key, nested_value in value.items()
                if nested_value is not None
            }
        else:
            finalized[key] = value
    return finalized


def _deduplicate(
    rows: list[dict[str, Any]], engine: str
) -> tuple[list[dict[str, Any]], int]:
    """Deduplicate by unified_id using the selected engine; return removed count.

    Output is sorted by `unified_id` so both engines (and every run) produce
    byte-stable ordering for idempotent downstream loads.
    """
    if engine == "pandas":
        frame = clean_frame_pandas(
            build_dataframe(rows),
            string_columns=STRING_COLUMNS,
            datetime_columns=DATETIME_COLUMNS,
            numeric_columns=NUMERIC_COLUMNS,
        )
        deduped = deduplicate_frame_pandas(frame, ["unified_id"], "updated_at")
        deduped = deduped.sort_values("unified_id")
        records = deduped.to_dict("records")
    else:
        frame = clean_frame(
            build_frame(rows),
            string_columns=STRING_COLUMNS,
            datetime_columns=DATETIME_COLUMNS,
            numeric_columns=NUMERIC_COLUMNS,
        )
        deduped = deduplicate_frame(frame, ["unified_id"], "updated_at")
        deduped = deduped.sort("unified_id")
        records = deduped.to_dicts()

    records = [_finalize_record(record) for record in records]
    return records, len(rows) - len(records)