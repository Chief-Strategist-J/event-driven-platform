package queries

const InsertSagaDefinition = `
INSERT INTO saga_definition_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    saga_name, total_steps, definition_json, is_active, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (tenant_id, saga_name) DO UPDATE SET
    total_steps = EXCLUDED.total_steps,
    definition_json = EXCLUDED.definition_json,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at;
`

const SelectSagaDefinition = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       saga_name, total_steps, definition_json, is_active, created_at, updated_at
FROM saga_definition_ledger
WHERE tenant_id = $1 AND public_id = $2;
`

const InsertSagaInstance = `
INSERT INTO saga_instance_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    saga_definition_id, current_step_index, state, deadline_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
);
`

const InsertSagaStep = `
INSERT INTO saga_step_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    saga_instance_id, step_index, step_name, step_type, state,
    command_payload, reply_payload, compensation_payload, deadline_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
);
`

const SelectSagaInstance = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       saga_definition_id, current_step_index, state, deadline_at, created_at, updated_at
FROM saga_instance_ledger
WHERE tenant_id = $1 AND public_id = $2;
`

const UpdateSagaInstanceState = `
UPDATE saga_instance_ledger
SET state = $1,
    current_step_index = $2,
    updated_at = $3
WHERE tenant_id = $4 AND public_id = $5;
`

const SelectSagaSteps = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       saga_instance_id, step_index, step_name, step_type, state,
       command_payload, reply_payload, compensation_payload, deadline_at, created_at, updated_at
FROM saga_step_ledger
WHERE tenant_id = $1 AND saga_instance_id = $2
ORDER BY step_index ASC;
`

const UpdateSagaStepState = `
UPDATE saga_step_ledger
SET state = $1,
    reply_payload = $2,
    updated_at = $3
WHERE tenant_id = $4 AND saga_instance_id = $5 AND step_index = $6;
`

const InsertAsyncCommandReply = `
INSERT INTO async_command_reply_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    command_id, outcome, is_late_reply, reply_payload, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (tenant_id, command_id) DO NOTHING;
`


// InsertOutboundCommandOutbox writes a command into the transactional outbox
// table (owned by the transactional-outbox feature) for the relay to publish.
const InsertOutboundCommandOutbox = `
INSERT INTO outbox (
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $12::jsonb, $13::jsonb, 'PENDING', NOW(), NOW()
);`
