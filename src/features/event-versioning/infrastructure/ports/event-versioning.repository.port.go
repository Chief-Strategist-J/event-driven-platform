package ports

import (
	"context"

	"event-driven-platform/src/features/event-versioning/contracts"
)

type EventVersioningRepositoryPort interface {
	SaveSchemaVersion(ctx context.Context, record contracts.SchemaVersionRecord) error
	GetLatestSchemaVersion(ctx context.Context, tenantID, streamName, eventType string) (*contracts.SchemaVersionRecord, error)
	GetSchemaVersion(ctx context.Context, tenantID, streamName, eventType string, version int) (*contracts.SchemaVersionRecord, error)
	ListSchemaVersions(ctx context.Context, tenantID, streamName, eventType string) ([]contracts.SchemaVersionRecord, error)

	SaveUpcaster(ctx context.Context, record contracts.UpcasterRecord) error
	GetUpcasterChain(ctx context.Context, tenantID, eventType string, fromVersion, toVersion int) ([]contracts.UpcasterRecord, error)
}

