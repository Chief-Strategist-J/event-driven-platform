-- migration:      0017
-- description:    add version column and update triggers for consumer_group_member and consumer_stored_offset
-- author:         system-architect
-- date:           2026-11-06T13:10:00Z
-- depends_on:     0016
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

ALTER TABLE consumer_group_member 
ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

ALTER TABLE consumer_stored_offset 
ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
