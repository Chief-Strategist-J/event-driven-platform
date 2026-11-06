package queries

const (
	GetActiveKEKQuery = `
SELECT id, public_id, tenant_id, key_name, key_version, kms_provider, kms_key_arn, state, created_at, updated_at
FROM encryption_kek_ledger
WHERE tenant_id = $1 AND state = 'ACTIVE'
ORDER BY key_version DESC
LIMIT 1;`

	InsertKEKQuery = `
INSERT INTO encryption_kek_ledger (
    id, public_id, tenant_id, key_name, key_version, kms_provider, kms_key_arn, state, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
);`

	GetDEKBySubjectQuery = `
SELECT id, public_id, tenant_id, subject_id, kek_public_id, encrypted_dek_blob, algorithm, state, created_at, rotated_at, shredded_at
FROM encryption_dek_ledger
WHERE tenant_id = $1 AND subject_id = $2
LIMIT 1;`

	GetDEKByPublicIDQuery = `
SELECT id, public_id, tenant_id, subject_id, kek_public_id, encrypted_dek_blob, algorithm, state, created_at, rotated_at, shredded_at
FROM encryption_dek_ledger
WHERE tenant_id = $1 AND public_id = $2
LIMIT 1;`

	InsertDEKQuery = `
INSERT INTO encryption_dek_ledger (
    id, public_id, tenant_id, subject_id, kek_public_id, encrypted_dek_blob, algorithm, state, created_at, rotated_at, shredded_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);`

	ShredDEKQuery = `
UPDATE encryption_dek_ledger
SET state = 'SHREDDED',
    encrypted_dek_blob = 'SHREDDED_ZEROIZED_KEY',
    shredded_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND subject_id = $2;`

	InsertShredAuditQuery = `
INSERT INTO crypto_shredding_audit_ledger (
    id, public_id, tenant_id, subject_id, dek_public_id, reason, requested_by, shredded_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
);`
)
