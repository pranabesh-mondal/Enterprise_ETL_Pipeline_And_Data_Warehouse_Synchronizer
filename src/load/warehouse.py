"""Warehouse engine factory + idempotent upsert writer - Week 3."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine

from src.load.tables import etl_watermarks, unified_records, warehouse_metadata
from src.transform.clean import standardize_datetime
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

UPSERT_COLUMNS = [c.name for c in unified_records.columns if c.name != "unified_id"]
REQUIRED_COLUMNS = ("unified_id", "source", "source_resource", "source_id")


def get_engine(database_url: str, **kwargs: Any) -> Engine:
    """Create a SQLAlchemy engine for any supported warehouse DSN."""
    options: dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite"):
        options["connect_args"] = {"check_same_thread": False}
    options.update(kwargs)
    engine = create_engine(database_url, **options)
    warehouse_metadata.create_all(engine)
    logger.info("Warehouse ready: %s", _redacted(database_url))
    return engine


def _redacted(database_url: str) -> str:
    if "://" in database_url and "@" in database_url:
        scheme, rest = database_url.split("://", 1)
        return f"{scheme}://***@{rest.split('@', 1)[1]}"
    return database_url


def coerce_record(record: dict[str, Any]) -> dict[str, Any]:
    """Coerce one unified dict into warehouse-safe column values."""
    missing = [key for key in REQUIRED_COLUMNS if record.get(key) in (None, "")]
    if missing:
        raise ValueError(f"record missing required columns: {missing}")

    out: dict[str, Any] = {}
    for key in ("unified_id", "source", "source_resource", "source_id",
                "entity_type", "name", "email", "currency", "status"):
        value = record.get(key)
        out[key] = str(value) if value is not None else None
    if out.get("status") is None:
        out["status"] = "unknown"

    minor = record.get("amount_minor")
    out["amount_minor"] = int(minor) if minor is not None else None
    amount = record.get("amount")
    out["amount"] = Decimal(str(amount)) if amount is not None else None

    for key in ("created_at", "updated_at", "ingested_at"):
        raw = record.get(key)
        if isinstance(raw, datetime):
            parsed = raw
        elif isinstance(raw, str):
            parsed = standardize_datetime(raw)
        else:
            parsed = None
        if parsed is not None and parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        out[key] = parsed

    attributes = record.get("attributes")
    if isinstance(attributes, str):
        try:
            attributes = json.loads(attributes)
        except ValueError:
            attributes = {"_raw": attributes}
    out["attributes"] = dict(attributes) if isinstance(attributes, dict) else None
    return out
class WarehouseLoader:
    """Idempotent upsert writer for unified records."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @property
    def dialect(self) -> str:
        return self.engine.dialect.name

    def upsert_records(self, records: list[dict[str, Any]]) -> dict[str, int]:
        """Upsert records keyed on `unified_id`; returns inserted/updated counts."""
        stats = {"input": len(records), "inserted": 0, "updated": 0, "rejected": 0}
        if not records:
            return stats
        coerced: list[dict[str, Any]] = []
        for index, record in enumerate(records):
            try:
                coerced.append(coerce_record(record))
            except (ValueError, TypeError) as exc:
                logger.warning("Rejecting record %d: %s", index, exc)
                stats["rejected"] += 1
        if not coerced:
            return stats
        if self.dialect == "snowflake":
            return self._merge_snowflake(coerced, stats)
        if self.dialect in ("postgresql", "sqlite"):
            return self._upsert_on_conflict(coerced, stats)
        return self._upsert_portable(coerced, stats)

    def _existing_ids(self, rows: list[dict[str, Any]]) -> set[str]:
        from sqlalchemy import select as sa_select

        with self.engine.connect() as conn:
            result = conn.execute(
                sa_select(unified_records.c.unified_id).where(
                    unified_records.c.unified_id.in_([r["unified_id"] for r in rows])
                )
            )
            return {row[0] for row in result}

    def _upsert_on_conflict(self, rows: list[dict[str, Any]],
                              stats: dict[str, int]) -> dict[str, int]:
        if self.dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            insert_fn = pg_insert
        else:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            insert_fn = sqlite_insert
        existing = self._existing_ids(rows)
        with self.engine.begin() as conn:
            stmt = insert_fn(unified_records).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["unified_id"],
                set_={col: stmt.excluded[col] for col in UPSERT_COLUMNS},
            )
            conn.execute(stmt)
        stats["updated"] = len([r for r in rows if r["unified_id"] in existing])
        stats["inserted"] = len(rows) - stats["updated"]
        logger.info("Upserted %d (%d new, %d upd) via %s ON CONFLICT",
                    len(rows), stats["inserted"], stats["updated"], self.dialect)
        return stats

    def _merge_snowflake(self, rows: list[dict[str, Any]],
                         stats: dict[str, int]) -> dict[str, int]:
        existing = self._existing_ids(rows)
        columns = ["unified_id", *UPSERT_COLUMNS]
        placeholders = ", ".join(f":{c}" for c in columns)
        updates = ", ".join(f"tgt.{c} = src.{c}" for c in UPSERT_COLUMNS)
        sql = (
            "MERGE INTO unified_records AS tgt USING "
            f"(SELECT {placeholders}) AS src "
            "ON tgt.unified_id = src.unified_id "
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
            f"VALUES ({placeholders})"
        )
        with self.engine.begin() as conn:
            for row in rows:
                conn.exec_driver_sql(sql, row)
        stats["updated"] = len([r for r in rows if r["unified_id"] in existing])
        stats["inserted"] = len(rows) - stats["updated"]
        logger.info("Upserted %d (%d new, %d upd) via Snowflake MERGE",
                    len(rows), stats["inserted"], stats["updated"])
        return stats

    def _upsert_portable(self, rows: list[dict[str, Any]],
                         stats: dict[str, int]) -> dict[str, int]:
        from sqlalchemy import insert as sa_insert
        from sqlalchemy import update as sa_update

        with self.engine.begin() as conn:
            for row in rows:
                updated = conn.execute(
                    sa_update(unified_records)
                    .where(unified_records.c.unified_id == row["unified_id"])
                    .values({k: v for k, v in row.items() if k != "unified_id"})
                ).rowcount
                if updated:
                    stats["updated"] += 1
                else:
                    conn.execute(sa_insert(unified_records).values(row))
                    stats["inserted"] += 1
        logger.info("Upserted %d (%d new, %d upd) via portable path",
                    len(rows), stats["inserted"], stats["updated"])
        return stats

    def get_watermark(self, source: str, resource: str) -> datetime | None:
        from sqlalchemy import select as sa_select

        with self.engine.connect() as conn:
            row = conn.execute(
                sa_select(etl_watermarks.c.high_water_mark).where(
                    etl_watermarks.c.source == source,
                    etl_watermarks.c.resource == resource,
                )
            ).first()
        return row[0] if row else None

    def set_watermark(self, source: str, resource: str,
                      high_water_mark: datetime | None,
                      run_id: str | None = None) -> None:
        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            updated = conn.execute(
                etl_watermarks.update()
                .where(etl_watermarks.c.source == source,
                       etl_watermarks.c.resource == resource)
                .values(high_water_mark=high_water_mark,
                        last_run_id=run_id, updated_at=now)
            ).rowcount
            if not updated:
                conn.execute(
                    etl_watermarks.insert().values(
                        source=source, resource=resource,
                        high_water_mark=high_water_mark,
                        last_run_id=run_id, updated_at=now)
                )
        logger.info("Watermark %s/%s -> %s", source, resource, high_water_mark)

    def count(self) -> int:
        from sqlalchemy import func, select as sa_select

        with self.engine.connect() as conn:
            value = conn.execute(
                sa_select(func.count()).select_from(unified_records)).scalar()
            return int(value or 0)

            result = conn.execute(
                sa_select(unified_records.c.unified_id).where(
                    unified_records.c.unified_id.in_([r["unified_id"] for r in rows])
                )
            )
            return {row[0] for row in result}

