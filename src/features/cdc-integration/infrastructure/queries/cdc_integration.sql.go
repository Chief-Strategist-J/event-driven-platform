package queries

const (
	InsertConnectorInventory = `
INSERT INTO cdc_connector_inventory (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    connector_name, slot_name, publication_name, mode, state,
    wal_retained_bytes, wal_lag_bytes, last_heartbeat_at, version,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0, 0, CURRENT_TIMESTAMP, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
`

	SelectConnectorByName = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       connector_name, slot_name, publication_name, mode, state,
       wal_retained_bytes, wal_lag_bytes, last_heartbeat_at, version,
       created_at, updated_at
FROM cdc_connector_inventory
WHERE connector_name = $1
`

	UpdateConnectorState = `
UPDATE cdc_connector_inventory
SET state = $1
WHERE connector_name = $2 AND version = $3
`

	UpdateConnectorWALLag = `
UPDATE cdc_connector_inventory
SET wal_retained_bytes = $1,
    wal_lag_bytes = $2
WHERE connector_name = $3
`

	InsertSignalLog = `
INSERT INTO cdc_signal_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    connector_name, signal_type, signal_payload, processed, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE, CURRENT_TIMESTAMP)
`

	MarkSignalProcessed = `
UPDATE cdc_signal_log
SET processed = TRUE,
    processed_at = CURRENT_TIMESTAMP
WHERE id = $1
`

	InsertHeartbeatLog = `
INSERT INTO cdc_heartbeat_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    connector_name, lsn_position, emitted_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
`

	UpdateLastHeartbeat = `
UPDATE cdc_connector_inventory
SET last_heartbeat_at = CURRENT_TIMESTAMP
WHERE connector_name = $1
`

	InsertCanarySent = `
INSERT INTO cdc_canary_reconciliation_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    connector_name, canary_event_id, sent_at, latency_ms, reconciled
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, 0, FALSE)
`

	UpdateCanaryReceived = `
UPDATE cdc_canary_reconciliation_log
SET received_at = CURRENT_TIMESTAMP,
    latency_ms = $1,
    reconciled = TRUE
WHERE canary_event_id = $2
`
)

