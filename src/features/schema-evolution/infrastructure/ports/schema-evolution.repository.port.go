package ports

import (
	"context"

	"github.com/google/uuid"

	"event-driven-platform/src/features/schema-evolution/contracts"
)

type SchemaEvolutionRepositoryPort interface {
	RegisterSchemaVersion(ctx context.Context, record contracts.SchemaVersionRecord) error
	GetSchemaLineage(ctx context.Context, tenantID string, subjectName string) ([]contracts.SchemaVersionRecord, error)
	GetActiveConsumersForSubject(ctx context.Context, tenantID string, subjectName string) ([]contracts.ConsumerSupportRecord, error)
	RegisterConsumerSupport(ctx context.Context, record contracts.ConsumerSupportRecord) error
	CreateDualPublishEntry(ctx context.Context, record contracts.DualPublishRecord) error
	GetDualPublishEntry(ctx context.Context, tenantID string, subjectName string) (*contracts.DualPublishRecord, error)
	UpdateDualPublishStatus(ctx context.Context, id uuid.UUID, tenantID string, status string, attested bool, zeroLagDays int) error
	RecordIncident(ctx context.Context, record contracts.EvolutionIncidentRecord) error
}

