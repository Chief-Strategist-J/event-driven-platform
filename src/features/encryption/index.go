package encryption

import (
	"database/sql"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/infrastructure/adapters"
	"event-driven-platform/src/features/encryption/infrastructure/ports"
	"event-driven-platform/src/features/encryption/service"
)

type EncryptionService = service.EncryptionService
type EncryptionRepositoryPort = ports.EncryptionRepositoryPort

type KEKRecord = contracts.KEKRecord
type DEKRecord = contracts.DEKRecord
type CryptoShreddingRecord = contracts.CryptoShreddingRecord
type EncryptedValue = contracts.EncryptedValue
type DecryptOutcome = contracts.DecryptOutcome

type EncryptFieldInput = contracts.EncryptFieldInput
type DecryptFieldInput = contracts.DecryptFieldInput
type DecryptFieldOutput = contracts.DecryptFieldOutput
type ShredSubjectInput = contracts.ShredSubjectInput
type BlindIndexInput = contracts.BlindIndexInput
type BlindIndexOutput = contracts.BlindIndexOutput

func NewEncryptionService(repo ports.EncryptionRepositoryPort, masterSecret ...string) *service.EncryptionService {
	return service.NewEncryptionService(repo, masterSecret...)
}

func NewService(db *sql.DB, masterSecret ...string) *service.EncryptionService {
	repo := adapters.NewPostgresEncryptionRepositoryAdapter(db)
	return service.NewEncryptionService(repo, masterSecret...)
}

func NewPostgresEncryptionRepositoryAdapter(db *sql.DB) ports.EncryptionRepositoryPort {
	return adapters.NewPostgresEncryptionRepositoryAdapter(db)
}

