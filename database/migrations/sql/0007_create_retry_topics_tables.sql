-- migration:      0007
-- description:    create retry topics ledger table conforming to 5-role identifiers and immutability triggers
-- author:         chief-strategist
-- date:           2026-11-04T18:00:00Z
-- depends_on:     0006
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         track multi-tier retry schedules, exponential backoff, failure classification, and dead-letter escalations conforming to 05-db-writing-rules.md. valid_until is nullable for unbounded retries.

CREATE TABLE IF NOT EXISTS retry_attempt_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    handler_id VARCHAR(128) NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL DEFAULT 1,
    attempt_number INT NOT NULL DEFAULT 1,
    max_attempts INT NOT NULL DEFAULT 3,
    backoff_ms BIGINT NOT NULL DEFAULT 1000,
    due_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'SCHEDULED',
    failure_classification VARCHAR(32) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    error_message TEXT NOT NULL DEFAULT '',
    error_code VARCHAR(64) NOT NULL DEFAULT '',
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_retry_attempt_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_retry_attempt_ledger_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_retry_attempt_ledger_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uq_retry_attempt_ledger_attempt UNIQUE (tenant_id, consumer_group, handler_id, event_id, attempt_number),
    CONSTRAINT ck_retry_attempt_ledger_public_id CHECK (public_id ~ '^try_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_retry_attempt_ledger_attempts CHECK (attempt_number >= 1 AND attempt_number <= max_attempts),
    CONSTRAINT ck_retry_attempt_ledger_version CHECK (aggregate_version >= 1),
    CONSTRAINT ck_retry_attempt_ledger_status CHECK (status IN ('SCHEDULED', 'RETRYING', 'EXHAUSTED', 'EXPIRED', 'RESOLVED')),
    CONSTRAINT ck_retry_attempt_ledger_class CHECK (failure_classification IN ('ENVIRONMENTAL', 'CONTENTION', 'RATE_LIMITED', 'MESSAGE_TRANSIENT', 'AMBIGUOUS_EFFECT', 'CAUSAL_WAIT', 'PERMANENT_DATA', 'BUSINESS_REJECT', 'HANDLER_BUG', 'CRASH_POISON', 'REFERENCE_DATA_MISSING')),
    CONSTRAINT chk_retry_tenant_id CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT chk_retry_source_service CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT chk_retry_external_id CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT chk_retry_consumer_group CHECK (btrim(consumer_group) = consumer_group AND consumer_group <> ''),
    CONSTRAINT chk_retry_handler_id CHECK (btrim(handler_id) = handler_id AND handler_id <> ''),
    CONSTRAINT chk_retry_event_id CHECK (btrim(event_id) = event_id AND event_id <> ''),
    CONSTRAINT chk_retry_aggregate_type CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT chk_retry_aggregate_id CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT chk_retry_idempotency_key CHECK (btrim(idempotency_key) = idempotency_key AND idempotency_key <> '')
);

CREATE INDEX IF NOT EXISTS idx_retry_attempt_ledger_due 
ON retry_attempt_ledger (tenant_id, status, due_at) 
WHERE status = 'SCHEDULED';

CREATE INDEX IF NOT EXISTS idx_retry_attempt_ledger_key 
ON retry_attempt_ledger (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id);

CREATE OR REPLACE FUNCTION fn_prevent_retry_attempt_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Immutable identifier modification rejected on retry_attempt_ledger';
    END IF;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_retry_attempt_immutable_update ON retry_attempt_ledger;
CREATE TRIGGER trg_prevent_retry_attempt_immutable_update
BEFORE UPDATE ON retry_attempt_ledger
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_retry_attempt_immutable_update();
