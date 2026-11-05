-- migration:      0003
-- description:    rollback dead letter quarantine and aggregate parking tables
-- author:         chief-strategist
-- date:           2026-11-04T14:35:00Z
-- depends_on:     0003
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop dead letter quarantine and aggregate parking tables

DROP TABLE IF EXISTS dead_letter_quarantine;
DROP TABLE IF EXISTS aggregate_parking_store;
