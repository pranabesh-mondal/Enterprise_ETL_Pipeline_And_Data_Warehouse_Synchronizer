-- Enterprise ETL warehouse DDL - Week 3, Day 1-3.
-- Target: PostgreSQL (primary). SQLite-compatible subset noted inline.
-- Snowflake: same columns; use VARIANT for `attributes` (see below).

CREATE TABLE IF NOT EXISTS unified_records (
    unified_id      VARCHAR(256)  PRIMARY KEY,  -- source:entity_type:source_id
    source          VARCHAR(64)   NOT NULL,     -- stripe | salesforce
    source_resource VARCHAR(64)   NOT NULL,     -- customers | charges | ...
    source_id       VARCHAR(256)  NOT NULL,     -- natural key in source system
    entity_type     VARCHAR(64)   NOT NULL,     -- customer | transaction | ...
    name            VARCHAR(512),
    email           VARCHAR(320),
    currency        CHAR(3),
    amount_minor    BIGINT,
    amount          NUMERIC(18, 2),
    status          VARCHAR(64)   NOT NULL DEFAULT 'unknown',
    created_at      TIMESTAMPTZ   NOT NULL,
    updated_at      TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ   NOT NULL,
    attributes      JSONB                       -- Snowflake: VARIANT
);

CREATE INDEX IF NOT EXISTS ix_unified_source_resource
    ON unified_records (source, source_resource);
CREATE INDEX IF NOT EXISTS ix_unified_entity_type
    ON unified_records (entity_type);

CREATE TABLE IF NOT EXISTS etl_watermarks (
    source           VARCHAR(64) NOT NULL,
    resource         VARCHAR(64) NOT NULL,
    high_water_mark  TIMESTAMPTZ,
    last_run_id      VARCHAR(64),
    updated_at       TIMESTAMPTZ,
    PRIMARY KEY (source, resource)
);
