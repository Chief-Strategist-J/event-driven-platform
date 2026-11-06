package ports

import (
	"context"

	"github.com/google/uuid"

	"event-driven-platform/src/features/cqrs/contracts"
)

type CQRSRepositoryPort interface {
	GetIdempotencyRecord(ctx context.Context, tenantID string, idempotencyKey string) (*contracts.CommandRecord, error)
	CreateIdempotencyRecord(ctx context.Context, record contracts.CommandRecord) error
	UpdateIdempotencyRecord(ctx context.Context, id uuid.UUID, tenantID string, status contracts.CommandStatus, responsePayload string, token string) error
	GetAggregateForUpdate(ctx context.Context, tenantID string, aggregateType string, aggregateID string) (*contracts.AggregateRecord, error)
	UpsertAggregateMutation(ctx context.Context, record contracts.AggregateRecord) error
	GetActiveSigningKey(ctx context.Context, tenantID string) (*contracts.TokenSigningKeyRecord, error)
	RegisterSigningKey(ctx context.Context, record contracts.TokenSigningKeyRecord) error
}
