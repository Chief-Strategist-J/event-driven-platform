-- migration:      0005
-- description:    create event versioning and upcaster registry tables conforming to 5-role identifiers and immutability triggers
-- author:         chief-strategist
-- date:           2026-11-04T15:30:00Z
-- depends_on:     0004
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         wire schema registry metadata, transitive compatibility mode, and pure upcaster chain registration conforming to EV-01 to EV-12 and 05-db-writing-rules.md

CREATE TABLE IF NOT EXISTS event_schema_version (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    stream_name VARCHAR(128) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    schema_version INT NOT NULL,
    schema_id VARCHAR(128) NOT NULL,
    format VARCHAR(32) NOT NULL,
    compatibility_mode VARCHAR(32) NOT NULL DEFAULT 'FULL_TRANSITIVE',
    schema_definition JSONB NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_event_schema_version PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_event_schema_version_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_event_schema_version_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_event_schema_version_stream_type_ver UNIQUE (tenant_id, stream_name, event_type, schema_version),
    CONSTRAINT uk_event_schema_version_id UNIQUE (tenant_id, schema_id),
    CONSTRAINT event_schema_public_id_ck CHECK (public_id ~ '^sch_[0-9a-zA-Z]{26}$'),
    CONSTRAINT event_schema_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT event_schema_source_service_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT event_schema_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT event_schema_stream_ck CHECK (stream_name = btrim(stream_name) AND stream_name <> ''),
    CONSTRAINT event_schema_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT event_schema_version_ck CHECK (schema_version >= 1),
    CONSTRAINT event_schema_id_ck CHECK (schema_id = btrim(schema_id) AND schema_id <> ''),
    CONSTRAINT event_schema_format_ck CHECK (format IN ('JSON_SCHEMA', 'AVRO', 'PROTOBUF')),
    CONSTRAINT event_schema_compat_ck CHECK (compatibility_mode IN ('FULL_TRANSITIVE', 'BACKWARD_TRANSITIVE', 'FORWARD_TRANSITIVE', 'NONE')),
    CONSTRAINT event_schema_def_ck CHECK (jsonb_typeof(schema_definition) = 'object'),
    CONSTRAINT event_schema_hash_ck CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_event_schema_lookup 
ON event_schema_version (tenant_id, event_type, schema_version DESC);

CREATE OR REPLACE FUNCTION trg_event_schema_version_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id THEN
        RAISE EXCEPTION 'Immutable fields cannot be updated on event_schema_version' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_event_schema_version_immutability ON event_schema_version;
CREATE TRIGGER trg_event_schema_version_immutability
BEFORE UPDATE ON event_schema_version
FOR EACH ROW EXECUTE FUNCTION trg_event_schema_version_immutable();

CREATE TABLE IF NOT EXISTS event_upcaster_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    from_version INT NOT NULL,
    to_version INT NOT NULL,
    transformer_name VARCHAR(128) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_event_upcaster_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_event_upcaster_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_event_upcaster_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_event_upcaster_chain UNIQUE (tenant_id, event_type, from_version, to_version),
    CONSTRAINT upcaster_public_id_ck CHECK (public_id ~ '^upc_[0-9a-zA-Z]{26}$'),
    CONSTRAINT upcaster_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT upcaster_source_service_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT upcaster_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT upcaster_event_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT upcaster_from_ver_ck CHECK (from_version >= 1),
    CONSTRAINT upcaster_to_ver_ck CHECK (to_version = from_version + 1),
    CONSTRAINT upcaster_name_ck CHECK (transformer_name = btrim(transformer_name) AND transformer_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_upcaster_chain 
ON event_upcaster_registry (tenant_id, event_type, from_version ASC)
WHERE is_active = TRUE;

CREATE OR REPLACE FUNCTION trg_event_upcaster_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id THEN
        RAISE EXCEPTION 'Immutable fields cannot be updated on event_upcaster_registry' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_event_upcaster_immutability ON event_upcaster_registry;
CREATE TRIGGER trg_event_upcaster_immutability
BEFORE UPDATE ON event_upcaster_registry
FOR EACH ROW EXECUTE FUNCTION trg_event_upcaster_immutable();
