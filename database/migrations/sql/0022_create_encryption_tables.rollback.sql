-- rollback:    0022
-- description: rollback encryption kek, dek, and crypto shredding ledgers

DROP INDEX IF EXISTS idx_crypto_shredding_subject_audit;
DROP INDEX IF EXISTS idx_encryption_dek_subject_lookup;
DROP INDEX IF EXISTS idx_encryption_kek_lookup;

DROP TABLE IF EXISTS crypto_shredding_audit_ledger CASCADE;
DROP TABLE IF EXISTS encryption_dek_ledger CASCADE;
DROP TABLE IF EXISTS encryption_kek_ledger CASCADE;
