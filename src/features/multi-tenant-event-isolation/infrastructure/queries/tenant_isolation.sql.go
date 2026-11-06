package queries

const (
	InsertTenantRegistry = `
INSERT INTO tenant_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    tier, lifecycle_state, home_region, allowed_regions, retention_class,
    max_throughput_eps, max_concurrency_lanes, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
`

	SelectTenantRegistry = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       tier, lifecycle_state, home_region, allowed_regions, retention_class,
       max_throughput_eps, max_concurrency_lanes, version, created_at, updated_at
FROM tenant_registry
WHERE tenant_id = $1
`

	UpdateTenantLifecycle = `
UPDATE tenant_registry
SET lifecycle_state = $1
WHERE tenant_id = $2 AND version = $3
`

	InsertQuotaBudget = `
INSERT INTO tenant_quota_budgets (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    held_keys_count, max_held_keys, quarantine_count, max_quarantine_budget,
    is_throttled, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (tenant_id) DO NOTHING
`

	SelectQuotaBudget = `
SELECT held_keys_count, max_held_keys, quarantine_count, max_quarantine_budget, is_throttled
FROM tenant_quota_budgets
WHERE tenant_id = $1
`

	UpdateQuotaCounts = `
UPDATE tenant_quota_budgets
SET held_keys_count = GREATEST(0, held_keys_count + $1),
    quarantine_count = GREATEST(0, quarantine_count + $2),
    is_throttled = $3
WHERE tenant_id = $4
`

	InsertDEKKeystore = `
INSERT INTO tenant_dek_keystore (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    subject_id, period_key, wrapped_dek, kek_key_id, algorithm, status, version,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'ACTIVE', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
`

	SelectDEK = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       subject_id, period_key, wrapped_dek, kek_key_id, algorithm, status,
       shredded_at, version, created_at, updated_at
FROM tenant_dek_keystore
WHERE tenant_id = $1 AND subject_id = $2 AND period_key = $3
`

	ShredDEK = `
UPDATE tenant_dek_keystore
SET status = 'SHREDDED',
    shredded_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND subject_id = $2 AND period_key = $3 AND status = 'ACTIVE'
`

	InsertAuditLog = `
INSERT INTO tenant_isolation_audit_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    action, details, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
`
)

