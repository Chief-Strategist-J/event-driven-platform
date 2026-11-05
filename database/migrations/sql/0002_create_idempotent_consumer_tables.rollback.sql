-- migration:      0002
-- description:    rollback consumer inbox and projection tables
-- author:         chief-strategist
-- date:           2026-11-04T13:40:00Z
-- depends_on:     0002
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop consumer inbox, projections, and partition offset tables

DROP TABLE IF EXISTS consumer_inbox;
DROP TABLE IF EXISTS consumer_projections;
DROP TABLE IF EXISTS consumer_partition_offsets;
