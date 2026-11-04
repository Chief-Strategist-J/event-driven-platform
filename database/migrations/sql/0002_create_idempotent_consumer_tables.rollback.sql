-- rollback migration: 0002
DROP TABLE IF EXISTS consumer_inbox;
DROP TABLE IF EXISTS consumer_projections;
DROP TABLE IF EXISTS consumer_partition_offsets;
