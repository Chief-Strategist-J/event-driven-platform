-- migration:      0008
-- description:    rollback consumer group membership and rebalance log tables
-- author:         chief-strategist
-- date:           2026-11-04T18:30:00Z
-- depends_on:     0008
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop consumer group member and rebalance log tables and triggers

DROP TRIGGER IF EXISTS trg_prevent_consumer_group_rebalance_log_modification ON consumer_group_rebalance_log;
DROP FUNCTION IF EXISTS fn_prevent_consumer_group_rebalance_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_consumer_group_member_immutable_update ON consumer_group_member;
DROP FUNCTION IF EXISTS fn_prevent_consumer_group_member_immutable_update();
DROP TABLE IF EXISTS consumer_group_rebalance_log;
DROP TABLE IF EXISTS consumer_group_member;
