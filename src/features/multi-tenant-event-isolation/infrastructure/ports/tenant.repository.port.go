package ports

import (
	"context"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
)

type TenantRepositoryPort interface {
	RegisterTenant(ctx context.Context, tenant contracts.TenantRecord) error
	GetTenant(ctx context.Context, tenantID string) (*contracts.TenantRecord, error)
	UpdateLifecycleState(ctx context.Context, tenantID string, state contracts.TenantLifecycleState, version int64) error
	UpdateQuotaBudget(ctx context.Context, tenantID string, deltaHeldKeys int, deltaQuarantine int, isThrottled bool) error
	GetQuotaBudget(ctx context.Context, tenantID string) (*contracts.QuotaCheckResult, error)
	AppendAuditLog(ctx context.Context, tenantID string, action string, sourceService string, externalID string, correlationID string, details map[string]interface{}) error
}

