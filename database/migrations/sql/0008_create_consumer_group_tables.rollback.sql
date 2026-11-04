DROP TRIGGER IF EXISTS trg_prevent_consumer_group_member_immutable_update ON consumer_group_member;
DROP FUNCTION IF EXISTS fn_prevent_consumer_group_member_immutable_update();
DROP TABLE IF EXISTS consumer_group_rebalance_log;
DROP TABLE IF EXISTS consumer_group_member;

