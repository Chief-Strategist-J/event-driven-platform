package queries

const InsertCatalogEntry = `
INSERT INTO domain_event_catalog_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    event_type, owning_aggregate, semantics_class, visibility, pii_classification,
    schema_fingerprint, is_active, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, event_type) DO UPDATE SET
    owning_aggregate = EXCLUDED.owning_aggregate,
    semantics_class = EXCLUDED.semantics_class,
    visibility = EXCLUDED.visibility,
    pii_classification = EXCLUDED.pii_classification,
    schema_fingerprint = EXCLUDED.schema_fingerprint,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at;
`

const SelectCatalogEntry = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       event_type, owning_aggregate, semantics_class, visibility, pii_classification,
       schema_fingerprint, is_active, created_at, updated_at
FROM domain_event_catalog_ledger
WHERE tenant_id = $1 AND event_type = $2;
`

const SelectAggregateVersion = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       aggregate_type, aggregate_id, current_version, is_terminal, created_at, updated_at
FROM aggregate_root_version_ledger
WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3;
`

const UpsertInitialAggregateVersion = `
INSERT INTO aggregate_root_version_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    aggregate_type, aggregate_id, current_version, is_terminal, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO NOTHING;
`

const UpdateAggregateVersionOCC = `
UPDATE aggregate_root_version_ledger
SET current_version = $1,
    is_terminal = $2,
    updated_at = $3
WHERE tenant_id = $4 AND aggregate_type = $5 AND aggregate_id = $6
  AND current_version = $7 AND is_terminal = FALSE;
`

const UpsertTransactionGroup = `
INSERT INTO transaction_group_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    transaction_group_id, expected_group_size, received_count, is_complete, created_at, completed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, 1, ($8 = 1), $9::timestamptz,
    CASE WHEN $8 = 1 THEN $9::timestamptz ELSE '1970-01-01 00:00:00+00'::timestamptz END
)
ON CONFLICT (tenant_id, transaction_group_id) DO UPDATE SET
    received_count = transaction_group_ledger.received_count + 1,
    is_complete = (transaction_group_ledger.received_count + 1 >= transaction_group_ledger.expected_group_size),
    completed_at = CASE 
        WHEN (transaction_group_ledger.received_count + 1 >= transaction_group_ledger.expected_group_size) THEN $9::timestamptz
        ELSE transaction_group_ledger.completed_at 
    END
RETURNING transaction_group_id, expected_group_size, received_count, is_complete;
`

const InsertCausalChainEntry = `
INSERT INTO causal_chain_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    event_id, causation_id, causation_depth, causation_path_hash, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (tenant_id, event_id) DO NOTHING;
`

const SelectCausalChainEntry = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       event_id, causation_id, causation_depth, causation_path_hash, created_at
FROM causal_chain_log
WHERE tenant_id = $1 AND event_id = $2;
`

