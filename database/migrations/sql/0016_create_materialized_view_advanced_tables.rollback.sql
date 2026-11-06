-- migration:      0016
-- description:    rollback materialized view aggregation contributions and financial period ledger tables
-- author:         chief-strategist
-- date:           2026-11-06T12:56:00Z
-- depends_on:     0016
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-13 advanced materialized view tables

DROP TRIGGER IF EXISTS trg_prevent_materialized_view_agg_contributions_mutation ON materialized_view_agg_contributions;
DROP FUNCTION IF EXISTS fn_prevent_materialized_view_agg_contributions_mutation();

DROP TABLE IF EXISTS materialized_view_period_ledger;
DROP TABLE IF EXISTS materialized_view_agg_contributions;
