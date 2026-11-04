DROP TRIGGER IF EXISTS trg_prevent_retry_attempt_immutable_update ON retry_attempt_ledger;
DROP FUNCTION IF EXISTS fn_prevent_retry_attempt_immutable_update();
DROP TABLE IF EXISTS retry_attempt_ledger;
