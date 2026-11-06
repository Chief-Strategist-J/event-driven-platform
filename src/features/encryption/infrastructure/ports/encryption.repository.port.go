package ports

import (
	"context"

	"event-driven-platform/src/features/encryption/contracts"
)

type EncryptionRepositoryPort interface {
	GetActiveKEK(ctx context.Context, tenantID string) (*contracts.KEKRecord, error)
	RegisterKEK(ctx context.Context, kek contracts.KEKRecord) error
	GetDEKBySubject(ctx context.Context, tenantID string, subjectID string) (*contracts.DEKRecord, error)
	GetDEKByPublicID(ctx context.Context, tenantID string, publicID string) (*contracts.DEKRecord, error)
	RegisterDEK(ctx context.Context, dek contracts.DEKRecord) error
	ShredSubjectDEK(ctx context.Context, in contracts.ShredSubjectInput, dekPublicID string) error
}
