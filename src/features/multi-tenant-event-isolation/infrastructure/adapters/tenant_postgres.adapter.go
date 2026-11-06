package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/infrastructure/queries"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/utils"
)

type PostgresTenantRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresTenantRepositoryAdapter(db *sql.DB) *PostgresTenantRepositoryAdapter {
	return &PostgresTenantRepositoryAdapter{db: db}
}

func (a *PostgresTenantRepositoryAdapter) RegisterTenant(ctx context.Context, tenant contracts.TenantRecord) error {
	allowedRegionsStr := strings.Join(tenant.AllowedRegions, ",")
	_, err := a.db.ExecContext(ctx, queries.InsertTenantRegistry,
		tenant.ID, tenant.PublicID, tenant.TenantID, tenant.SourceService, tenant.ExternalID, tenant.CorrelationID,
		string(tenant.Tier), string(tenant.LifecycleState), tenant.HomeRegion, allowedRegionsStr, tenant.RetentionClass,
		tenant.MaxThroughputEPS, tenant.MaxConcurrencyLanes,
	)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to insert tenant registry: %v", err))
	}

	quotaID, _ := utils.GenerateUUIDv7()
	quotaPublicID := fmt.Sprintf("qta_%s", strings.ReplaceAll(quotaID, "-", "")[:20])
	_, qErr := a.db.ExecContext(ctx, queries.InsertQuotaBudget,
		quotaID, quotaPublicID, tenant.TenantID, tenant.SourceService, tenant.ExternalID, tenant.CorrelationID,
		0, 1000, 0, 500, false,
	)
	if qErr != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to initialize tenant quota budget: %v", qErr))
	}

	return nil
}

func (a *PostgresTenantRepositoryAdapter) GetTenant(ctx context.Context, tenantID string) (*contracts.TenantRecord, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectTenantRegistry, tenantID)

	var rec contracts.TenantRecord
	var tierStr, stateStr, allowedStr string
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&tierStr, &stateStr, &rec.HomeRegion, &allowedStr, &rec.RetentionClass,
		&rec.MaxThroughputEPS, &rec.MaxConcurrencyLanes, &rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError(fmt.Sprintf("tenant not found: %s", tenantID))
		}
		return nil, errors.NewInternalError(fmt.Sprintf("failed to query tenant registry: %v", err))
	}

	rec.Tier = contracts.TenancyTier(tierStr)
	rec.LifecycleState = contracts.TenantLifecycleState(stateStr)
	parts := strings.Split(allowedStr, ",")
	rec.AllowedRegions = make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			rec.AllowedRegions = append(rec.AllowedRegions, trimmed)
		}
	}

	return &rec, nil
}

func (a *PostgresTenantRepositoryAdapter) UpdateLifecycleState(ctx context.Context, tenantID string, state contracts.TenantLifecycleState, version int64) error {
	res, err := a.db.ExecContext(ctx, queries.UpdateTenantLifecycle, string(state), tenantID, version)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update tenant lifecycle: %v", err))
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return errors.NewConflictError(fmt.Sprintf("tenant lifecycle update conflict or not found: %s (v%d)", tenantID, version))
	}
	return nil
}

func (a *PostgresTenantRepositoryAdapter) UpdateQuotaBudget(ctx context.Context, tenantID string, deltaHeldKeys int, deltaQuarantine int, isThrottled bool) error {
	_, err := a.db.ExecContext(ctx, queries.UpdateQuotaCounts, deltaHeldKeys, deltaQuarantine, isThrottled, tenantID)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update quota counts: %v", err))
	}
	return nil
}

func (a *PostgresTenantRepositoryAdapter) GetQuotaBudget(ctx context.Context, tenantID string) (*contracts.QuotaCheckResult, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectQuotaBudget, tenantID)
	var held, maxHeld, quar, maxQuar int
	var throttled bool
	err := row.Scan(&held, &maxHeld, &quar, &maxQuar, &throttled)
	if err != nil {
		if err == sql.ErrNoRows {
			return &contracts.QuotaCheckResult{
				Allowed:            true,
				Throttled:          false,
				CurrentHeldKeys:    0,
				MaxHeldKeys:        1000,
				QuarantineCount:    0,
				MaxQuarantineLimit: 500,
			}, nil
		}
		return nil, errors.NewInternalError(fmt.Sprintf("failed to get quota budget: %v", err))
	}

	return &contracts.QuotaCheckResult{
		Allowed:            !throttled,
		Throttled:          throttled,
		CurrentHeldKeys:    held,
		MaxHeldKeys:        maxHeld,
		QuarantineCount:    quar,
		MaxQuarantineLimit: maxQuar,
	}, nil
}

func (a *PostgresTenantRepositoryAdapter) AppendAuditLog(ctx context.Context, tenantID string, action string, sourceService string, externalID string, correlationID string, details map[string]interface{}) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		detailsJSON = []byte("{}")
	}

	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("aud_%s", strings.ReplaceAll(id, "-", "")[:20])

	_, execErr := a.db.ExecContext(ctx, queries.InsertAuditLog,
		id, publicID, tenantID, sourceService, externalID, correlationID, action, detailsJSON,
	)
	if execErr != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to append tenant isolation audit log: %v", execErr))
	}
	return nil
}
