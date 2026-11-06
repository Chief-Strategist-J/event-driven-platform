-- migration:      0019_rollback
-- description:    drop cqrs command idempotency ledger, aggregate root ledger, and token signing key registry
-- author:         chief-strategist
-- date:           2026-11-06T14:06:00Z
-- depends_on:     0018

DROP TRIGGER IF EXISTS trg_cqrs_aggregate_immutable ON cqrs_aggregate_root_ledger;
DROP FUNCTION IF EXISTS trg_prevent_cqrs_aggregate_immutable_update();

DROP TRIGGER IF EXISTS trg_cqrs_idempotency_immutable ON cqrs_command_idempotency_ledger;
DROP FUNCTION IF EXISTS trg_prevent_cqrs_idempotency_immutable_update();

DROP TABLE IF EXISTS cqrs_token_signing_key_registry;
DROP TABLE IF EXISTS cqrs_aggregate_root_ledger;
DROP TABLE IF EXISTS cqrs_command_idempotency_ledger;

