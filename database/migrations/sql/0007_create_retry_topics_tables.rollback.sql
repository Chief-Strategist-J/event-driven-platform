-- migration:      0007
-- description:    rollback retry topics ledger table
-- author:         chief-strategist
-- date:           2026-11-04T18:00:00Z
-- depends_on:     0007
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop retry attempt ledger table and immutability triggers

DROP TRIGGER IF EXISTS trg_prevent_retry_attempt_immutable_update ON retry_attempt_ledger;
DROP FUNCTION IF EXISTS fn_prevent_retry_attempt_immutable_update();
DROP TABLE IF EXISTS retry_attempt_ledger;
