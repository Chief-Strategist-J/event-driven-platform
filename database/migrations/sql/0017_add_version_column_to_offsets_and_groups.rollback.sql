-- rollback:       0017
-- description:    rollback version column on consumer_group_member and consumer_stored_offset

ALTER TABLE consumer_group_member DROP COLUMN IF EXISTS version;
ALTER TABLE consumer_stored_offset DROP COLUMN IF EXISTS version;
