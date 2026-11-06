-- migration:      0013
-- description:    rollback cdc connector inventory, signal log, heartbeat log, and canary reconciliation tables
-- author:         chief-strategist
-- date:           2026-11-06T10:22:00Z
-- depends_on:     0013
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-11 cdc integration tables

DROP TRIGGER IF EXISTS trg_prevent_cdc_connector_inventory_immutable_update ON cdc_connector_inventory;
DROP FUNCTION IF EXISTS fn_prevent_cdc_connector_inventory_immutable_update();

DROP TABLE IF EXISTS cdc_canary_reconciliation_log;
DROP TABLE IF EXISTS cdc_heartbeat_log;
DROP TABLE IF EXISTS cdc_signal_log;
DROP TABLE IF EXISTS cdc_connector_inventory;

