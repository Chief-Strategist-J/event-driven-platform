package queries

const (
	GetLatestRecordQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       shard_id, sequence_number, actor_id, actor_type, action_name,
       target_type, target_id, outcome, compliance_class, prev_record_hash,
       record_hash, canonical_payload, occurred_at, ingested_at
FROM audit_trail_ledger
WHERE tenant_id = $1 AND shard_id = $2
ORDER BY sequence_number DESC
LIMIT 1;`

	InsertAuditRecordQuery = `
INSERT INTO audit_trail_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    shard_id, sequence_number, actor_id, actor_type, action_name,
    target_type, target_id, outcome, compliance_class, prev_record_hash,
    record_hash, canonical_payload, occurred_at, ingested_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
);`

	GetShardRecordsRangeQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       shard_id, sequence_number, actor_id, actor_type, action_name,
       target_type, target_id, outcome, compliance_class, prev_record_hash,
       record_hash, canonical_payload, occurred_at, ingested_at
FROM audit_trail_ledger
WHERE tenant_id = $1 AND shard_id = $2 AND sequence_number >= $3 AND sequence_number <= $4
ORDER BY sequence_number ASC;`

	GetLatestCheckpointQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       shard_id, checkpoint_seq, start_sequence, end_sequence, record_count,
       merkle_root_hash, head_record_hash, anchor_signature, status, created_at
FROM audit_trail_checkpoint_ledger
WHERE tenant_id = $1 AND shard_id = $2
ORDER BY checkpoint_seq DESC
LIMIT 1;`

	InsertCheckpointQuery = `
INSERT INTO audit_trail_checkpoint_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    shard_id, checkpoint_seq, start_sequence, end_sequence, record_count,
    merkle_root_hash, head_record_hash, anchor_signature, status, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	InsertAccessLogQuery = `
INSERT INTO audit_trail_access_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    auditor_id, case_id, query_scope, reason, accessed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);`

	QueryAuditRecordsBase = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       shard_id, sequence_number, actor_id, actor_type, action_name,
       target_type, target_id, outcome, compliance_class, prev_record_hash,
       record_hash, canonical_payload, occurred_at, ingested_at
FROM audit_trail_ledger
WHERE tenant_id = $1 AND shard_id = $2
ORDER BY sequence_number DESC
LIMIT $3;`
)
