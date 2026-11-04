package queries

const (
	FlowUpsertGroupMember = `
INSERT INTO consumer_group_member (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, member_id, instance_id, client_host, assignment_strategy,
    assigned_partitions, generation_id, heartbeat_at, status
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13, $14, $15
)
ON CONFLICT (tenant_id, group_id, member_id) DO UPDATE SET
    client_host = EXCLUDED.client_host,
    instance_id = EXCLUDED.instance_id,
    assignment_strategy = EXCLUDED.assignment_strategy,
    heartbeat_at = EXCLUDED.heartbeat_at,
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, public_id, created_at, updated_at;
`

	FlowGetGroupMember = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, member_id, instance_id, client_host, assignment_strategy,
    assigned_partitions, generation_id, heartbeat_at, status,
    created_at, updated_at
FROM consumer_group_member
WHERE tenant_id = $1 AND group_id = $2 AND member_id = $3;
`

	FlowUpdateMemberHeartbeat = `
UPDATE consumer_group_member
SET heartbeat_at = $4, status = 'STABLE', updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND group_id = $2 AND member_id = $3
RETURNING generation_id, status;
`

	FlowUpdateMemberPartitions = `
UPDATE consumer_group_member
SET assigned_partitions = $4, generation_id = $5, status = 'STABLE', updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND group_id = $2 AND member_id = $3;
`

	FlowInsertRebalanceLog = `
INSERT INTO consumer_group_rebalance_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, generation_id, rebalance_reason, member_count,
    revoked_partitions, assigned_partitions, duration_ms
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11, $12, $13
) RETURNING created_at;
`

	FlowListGroupMembers = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, member_id, instance_id, client_host, assignment_strategy,
    assigned_partitions, generation_id, heartbeat_at, status,
    created_at, updated_at
FROM consumer_group_member
WHERE tenant_id = $1 AND group_id = $2
ORDER BY member_id ASC;
`
)
