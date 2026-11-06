-- rollback migration: 0025
-- description:        drop async command reply ledger, saga step ledger, saga instance ledger, and saga definition ledger

DROP TRIGGER IF EXISTS trg_async_reply_no_update ON async_command_reply_ledger;
DROP FUNCTION IF EXISTS trg_prevent_async_reply_mutation();

DROP TABLE IF EXISTS async_command_reply_ledger;
DROP TABLE IF EXISTS saga_step_ledger;
DROP TABLE IF EXISTS saga_instance_ledger;
DROP TABLE IF EXISTS saga_definition_ledger;
