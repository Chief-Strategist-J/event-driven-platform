package ports

import (
	"context"

	"event-driven-platform/src/features/domain-event-design/contracts"
)

type DomainEventRepositoryPort interface {
	SaveCatalogEntry(ctx context.Context, entry contracts.EventCatalogEntry) error
	GetCatalogEntry(ctx context.Context, tenantID string, eventType string) (contracts.EventCatalogEntry, error)
	GetAggregateVersion(ctx context.Context, tenantID string, aggType string, aggID string) (contracts.AggregateRootVersion, error)
	ReserveAggregateVersion(ctx context.Context, cmd contracts.ReserveAggregateVersionCommand) (contracts.ReserveAggregateVersionResult, error)
	UpsertTransactionGroup(ctx context.Context, cmd contracts.RegisterTransactionGroupPartCommand) (contracts.RegisterTransactionGroupPartResult, error)
	RecordCausalEntry(ctx context.Context, entry contracts.CausalChainEntry) error
	GetCausalEntry(ctx context.Context, tenantID string, eventID string) (contracts.CausalChainEntry, error)
}

