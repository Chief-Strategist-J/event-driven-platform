package queries

const (
	FlowInsertSchemaVersion = `
INSERT INTO schema_evolution_version_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    subject_name, schema_version, compatibility_mode, schema_fingerprint,
    schema_definition, status, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
);`

	FlowSelectSchemaLineage = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       subject_name, schema_version, compatibility_mode, schema_fingerprint,
       schema_definition, status, version, created_at, updated_at
FROM schema_evolution_version_registry
WHERE tenant_id = $1 AND subject_name = $2
ORDER BY schema_version ASC;`

	FlowSelectConsumersForSubject = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       consumer_group, subject_name, min_supported_version, max_supported_version,
       run_cadence, status, version, created_at, updated_at
FROM schema_evolution_consumer_registry
WHERE tenant_id = $1 AND subject_name = $2 AND status = 'ACTIVE';`

	FlowUpsertConsumerSupport = `
INSERT INTO schema_evolution_consumer_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, subject_name, min_supported_version, max_supported_version,
    run_cadence, status, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, consumer_group, subject_name) DO UPDATE
SET min_supported_version = EXCLUDED.min_supported_version,
    max_supported_version = EXCLUDED.max_supported_version,
    run_cadence = EXCLUDED.run_cadence,
    status = EXCLUDED.status,
    version = schema_evolution_consumer_registry.version + 1,
    updated_at = CURRENT_TIMESTAMP;`

	FlowInsertDualPublish = `
INSERT INTO schema_evolution_dual_publish_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    subject_name, legacy_topic, modern_topic, sunset_deadline,
    status, decommission_attested, zero_lag_days, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FlowSelectDualPublish = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       subject_name, legacy_topic, modern_topic, sunset_deadline,
       status, decommission_attested, zero_lag_days, version, created_at, updated_at
FROM schema_evolution_dual_publish_ledger
WHERE tenant_id = $1 AND subject_name = $2;`

	FlowUpdateDualPublish = `
UPDATE schema_evolution_dual_publish_ledger
SET status = $3,
    decommission_attested = $4,
    zero_lag_days = $5,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND tenant_id = $2;`

	FlowInsertEvolutionIncident = `
INSERT INTO schema_evolution_incident_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    subject_name, consumer_group, incident_class, payload_fingerprint,
    error_message, status, occurred_at, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);`
)

