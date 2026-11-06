-- migration:      0021
-- description:    create audit trail ledger, hash chains, and anchor checkpoints
-- author:         chief-strategist
-- date:           2026-11-06T15:07:00Z
-- depends_on:     0020
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce AU-01 (single logical record), AU-03 (outcomes recorded), AU-04 (integrity anchoring), AU-05 (canonical bytes verbatim), AU-06 (sharded hash chains), and AU-10 (secret exclusion) per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS audit_trail_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    shard_id VARCHAR(64) NOT NULL,
    sequence_number BIGINT NOT NULL,
    actor_id VARCHAR(128) NOT NULL,
    actor_type VARCHAR(32) NOT NULL,
    action_name VARCHAR(128) NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_id VARCHAR(128) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    compliance_class VARCHAR(32) NOT NULL,
    prev_record_hash VARCHAR(64) NOT NULL,
    record_hash VARCHAR(64) NOT NULL,
    canonical_payload TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_audit_trail_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_audit_trail_public_id UNIQUE (public_id),
    CONSTRAINT uk_audit_trail_shard_seq UNIQUE (tenant_id, shard_id, sequence_number),
    CONSTRAINT uk_audit_trail_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_audit_trail_public_id_format CHECK (public_id ~ '^aud_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_audit_trail_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_audit_trail_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_audit_trail_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_audit_trail_shard_id_btrim CHECK (btrim(shard_id) = shard_id AND shard_id <> ''),
    CONSTRAINT ck_audit_trail_sequence_pos CHECK (sequence_number > 0),
    CONSTRAINT ck_audit_trail_actor_id_btrim CHECK (btrim(actor_id) = actor_id AND actor_id <> ''),
    CONSTRAINT ck_audit_trail_actor_type CHECK (actor_type IN ('USER', 'SERVICE', 'SYSTEM', 'API_KEY')),
    CONSTRAINT ck_audit_trail_action_name_btrim CHECK (btrim(action_name) = action_name AND action_name <> ''),
    CONSTRAINT ck_audit_trail_target_type_btrim CHECK (btrim(target_type) = target_type AND target_type <> ''),
    CONSTRAINT ck_audit_trail_target_id_btrim CHECK (btrim(target_id) = target_id AND target_id <> ''),
    CONSTRAINT ck_audit_trail_outcome CHECK (outcome IN ('SUCCEEDED', 'DENIED', 'FAILED', 'INTENT')),
    CONSTRAINT ck_audit_trail_class CHECK (compliance_class IN ('CLASS_A', 'CLASS_B', 'CLASS_C', 'CLASS_D')),
    CONSTRAINT ck_audit_trail_prev_hash_len CHECK (length(prev_record_hash) = 64),
    CONSTRAINT ck_audit_trail_record_hash_len CHECK (length(record_hash) = 64)
);

CREATE TABLE IF NOT EXISTS audit_trail_checkpoint_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    shard_id VARCHAR(64) NOT NULL,
    checkpoint_seq BIGINT NOT NULL,
    start_sequence BIGINT NOT NULL,
    end_sequence BIGINT NOT NULL,
    record_count BIGINT NOT NULL,
    merkle_root_hash VARCHAR(64) NOT NULL,
    head_record_hash VARCHAR(64) NOT NULL,
    anchor_signature VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ANCHORED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_audit_trail_checkpoint_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_audit_trail_checkpoint_public_id UNIQUE (public_id),
    CONSTRAINT uk_audit_trail_checkpoint_shard_seq UNIQUE (tenant_id, shard_id, checkpoint_seq),
    CONSTRAINT uk_audit_trail_checkpoint_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_audit_trail_checkpoint_public_id_format CHECK (public_id ~ '^ack_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_audit_trail_checkpoint_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_audit_trail_checkpoint_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_audit_trail_checkpoint_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_audit_trail_checkpoint_shard_id_btrim CHECK (btrim(shard_id) = shard_id AND shard_id <> ''),
    CONSTRAINT ck_audit_trail_checkpoint_seq_pos CHECK (checkpoint_seq > 0),
    CONSTRAINT ck_audit_trail_checkpoint_range_valid CHECK (start_sequence > 0 AND end_sequence >= start_sequence AND record_count > 0),
    CONSTRAINT ck_audit_trail_checkpoint_merkle_len CHECK (length(merkle_root_hash) = 64),
    CONSTRAINT ck_audit_trail_checkpoint_head_len CHECK (length(head_record_hash) = 64),
    CONSTRAINT ck_audit_trail_checkpoint_status CHECK (status IN ('ANCHORED', 'VERIFIED', 'TAMPERED'))
);

CREATE TABLE IF NOT EXISTS audit_trail_access_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    auditor_id VARCHAR(128) NOT NULL,
    case_id VARCHAR(64) NOT NULL,
    query_scope TEXT NOT NULL,
    reason TEXT NOT NULL,
    accessed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_audit_trail_access_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_audit_trail_access_public_id UNIQUE (public_id),
    CONSTRAINT ck_audit_trail_access_public_id_format CHECK (public_id ~ '^ata_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_audit_trail_access_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_audit_trail_access_auditor_id_btrim CHECK (btrim(auditor_id) = auditor_id AND auditor_id <> ''),
    CONSTRAINT ck_audit_trail_access_case_id_btrim CHECK (btrim(case_id) = case_id AND case_id <> '')
);

CREATE INDEX IF NOT EXISTS idx_audit_trail_shard_seq
    ON audit_trail_ledger (tenant_id, shard_id, sequence_number DESC);

CREATE INDEX IF NOT EXISTS idx_audit_trail_target_lookup
    ON audit_trail_ledger (tenant_id, target_type, target_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_audit_trail_actor_lookup
    ON audit_trail_ledger (tenant_id, actor_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_audit_checkpoint_lookup
    ON audit_trail_checkpoint_ledger (tenant_id, shard_id, checkpoint_seq DESC);

-- AU-01 & AU-05: Strict Immutability Trigger (Prevent any UPDATE or DELETE)
CREATE OR REPLACE FUNCTION trg_prevent_audit_trail_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Audit records are strictly immutable and cannot be updated or deleted';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_trail_no_update ON audit_trail_ledger;
CREATE TRIGGER trg_audit_trail_no_update
    BEFORE UPDATE OR DELETE ON audit_trail_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_audit_trail_mutation();

DROP TRIGGER IF EXISTS trg_audit_checkpoint_no_update ON audit_trail_checkpoint_ledger;
CREATE TRIGGER trg_audit_checkpoint_no_update
    BEFORE UPDATE OR DELETE ON audit_trail_checkpoint_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_audit_trail_mutation();

DROP TRIGGER IF EXISTS trg_audit_access_no_update ON audit_trail_access_log;
CREATE TRIGGER trg_audit_access_no_update
    BEFORE UPDATE OR DELETE ON audit_trail_access_log
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_audit_trail_mutation();
