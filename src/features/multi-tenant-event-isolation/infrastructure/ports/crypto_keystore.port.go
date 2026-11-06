package ports

import (
	"context"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
)

type CryptoKeystorePort interface {
	StoreDEK(ctx context.Context, dek contracts.DEKRecord) error
	GetDEK(ctx context.Context, tenantID string, subjectID string, periodKey string) (*contracts.DEKRecord, error)
	ShredDEK(ctx context.Context, tenantID string, subjectID string, periodKey string) error
	EncryptField(ctx context.Context, tenantID string, subjectID string, periodKey string, plaintext []byte) ([]byte, error)
	DecryptField(ctx context.Context, tenantID string, subjectID string, periodKey string, ciphertext []byte) ([]byte, error)
}

