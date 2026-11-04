-- migration:      0001
-- description:    rollback transactional outbox table
-- author:         chief-strategist
-- date:           2026-11-04T10:30:00Z
-- depends_on:     NONE
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback initial schema for transactional-outbox feature

DROP TABLE IF EXISTS outbox;

