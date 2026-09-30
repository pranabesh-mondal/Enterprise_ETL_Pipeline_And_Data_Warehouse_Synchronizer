from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Index,
    MetaData,
    Numeric,
    String,
    Table,
)

warehouse_metadata = MetaData()


unified_records = Table(
    "unified_records",
    warehouse_metadata,
    Column("unified_id", String(256), primary_key=True),
    Column("source", String(64), nullable=False),
    Column("source_resource", String(64), nullable=False),
    Column("source_id", String(256), nullable=False),
    Column("entity_type", String(64), nullable=False),
    Column("name", String(512), nullable=True),
    Column("email", String(320), nullable=True),
    Column("currency", String(3), nullable=True),
    Column("amount_minor", BigInteger, nullable=True),
    Column("amount", Numeric(18, 2), nullable=True),
    Column("status", String(64), nullable=False, default="unknown"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=True),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
    Column("attributes", JSON, nullable=True),
    Index("ix_unified_source_resource", "source", "source_resource"),
    Index("ix_unified_entity_type", "entity_type"),
)

etl_watermarks = Table(
    "etl_watermarks",
    warehouse_metadata,
    Column("source", String(64), primary_key=True),
    Column("resource", String(64), primary_key=True),
    Column("high_water_mark", DateTime(timezone=True), nullable=True),
    Column("last_run_id", String(64), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=True),
)

WAREHOUSE_TABLES = {
    "unified_records": unified_records,
    "etl_watermarks": etl_watermarks,
}