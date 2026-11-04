-- migration: 0003 rollback
DROP TABLE IF EXISTS dead_letter_quarantine;
DROP TABLE IF EXISTS aggregate_parking_store;

