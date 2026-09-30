CREATE TABLE IF NOT EXISTS unified_records (
    unified_id      VARCHAR(256)  PRIMARY KEY,
    source          VARCHAR(64)   NOT NULL,
    source_resource VARCHAR(64)   NOT NULL,
    source_id       VARCHAR(256)  NOT NULL,
    entity_type     VARCHAR(64)   NOT NULL,
    name            VARCHAR(512),
    email           VARCHAR(320),
    currency        CHAR(3),
    amount_minor    BIGINT,
    amount          NUMERIC(18, 2),
    status          VARCHAR(64)   NOT NULL DEFAULT 'unknown',
    created_at      TIMESTAMPTZ   NOT NULL,
    updated_at      TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ   NOT NULL,
    attributes      JSONB
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