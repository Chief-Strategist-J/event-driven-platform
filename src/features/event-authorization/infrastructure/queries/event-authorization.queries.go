package queries

const (
	UpsertPolicyQuery = `
INSERT INTO auth_policy_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    policy_name, principal_id, principal_type, resource_type, resource_pattern,
    operation, effect, state, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (tenant_id, source_service, external_id) DO UPDATE SET
    policy_name = EXCLUDED.policy_name,
    resource_pattern = EXCLUDED.resource_pattern,
    operation = EXCLUDED.operation,
    effect = EXCLUDED.effect,
    state = EXCLUDED.state,
    updated_at = EXCLUDED.updated_at;
`

	SelectPolicyByPublicIDQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       policy_name, principal_id, principal_type, resource_type, resource_pattern,
       operation, effect, state, created_at, updated_at
FROM auth_policy_ledger
WHERE public_id = $1;
`

	SelectMatchingPoliciesQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       policy_name, principal_id, principal_type, resource_type, resource_pattern,
       operation, effect, state, created_at, updated_at
FROM auth_policy_ledger
WHERE tenant_id = $1 AND principal_id = $2 AND resource_type = $3 AND state = 'ACTIVE'
  AND (operation = $4 OR operation = '*')
ORDER BY effect DESC, created_at ASC;
`

	CountTopicWritersQuery = `
SELECT COUNT(DISTINCT principal_id)
FROM auth_policy_ledger
WHERE tenant_id = $1 AND resource_type = 'TOPIC' AND resource_pattern = $2
  AND operation = 'PUBLISH' AND effect = 'ALLOW' AND state = 'ACTIVE';
`

	UpdatePolicyStateQuery = `
UPDATE auth_policy_ledger
SET state = $2, updated_at = CURRENT_TIMESTAMP
WHERE public_id = $1;
`

	InsertAuditLogQuery = `
INSERT INTO auth_audit_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    principal_id, principal_type, resource_type, resource_name,
    operation, decision, matched_policy_id, reason, evaluated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
);
`

	SelectAuditLogByPublicIDQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       principal_id, principal_type, resource_type, resource_name,
       operation, decision, matched_policy_id, reason, evaluated_at
FROM auth_audit_log
WHERE public_id = $1;
`
)
