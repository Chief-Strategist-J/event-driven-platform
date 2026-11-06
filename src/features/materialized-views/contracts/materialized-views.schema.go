package contracts

import (
	"strings"
	"time"

	"event-driven-platform/src/shared/errors"
	"github.com/google/uuid"
)

func NormalizeRegisterViewInput(in RegisterViewInput) (RegisterViewInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.ViewName = strings.TrimSpace(out.ViewName)
	out.ActiveTableName = strings.TrimSpace(out.ActiveTableName)
	out.ViewClass = ViewClass(strings.TrimSpace(string(out.ViewClass)))
	out.PrimaryStore = StorageTarget(strings.TrimSpace(string(out.PrimaryStore)))
	out.ConsistencyMode = ConsistencyMode(strings.TrimSpace(string(out.ConsistencyMode)))

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.SourceService == "" {
		return out, errors.NewValidationError("source_service must not be empty")
	}
	if out.ExternalID == "" {
		return out, errors.NewValidationError("external_id must not be empty")
	}
	if out.ViewName == "" {
		return out, errors.NewValidationError("view_name must not be empty")
	}
	if out.ActiveTableName == "" {
		out.ActiveTableName = out.ViewName
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	switch out.ViewClass {
	case ViewClassState, ViewClassAgg, ViewClassJoin, ViewClassSearch, ViewClassKV, ViewClassReport:
	default:
		return out, errors.NewValidationError("invalid view_class")
	}

	switch out.PrimaryStore {
	case StorageTargetPostgres, StorageTargetRedis, StorageTargetOpenSearch:
	case "":
		out.PrimaryStore = StorageTargetPostgres
	default:
		return out, errors.NewValidationError("invalid primary_store")
	}

	switch out.ConsistencyMode {
	case ConsistencyModeEventual, ConsistencyModeTokenBounded, ConsistencyModeStrictSerial:
	case "":
		out.ConsistencyMode = ConsistencyModeEventual
	default:
		return out, errors.NewValidationError("invalid consistency_mode")
	}

	if out.FreshnessSLOMs <= 0 {
		out.FreshnessSLOMs = 1000
	}

	return out, nil
}

func NormalizeApplyProjectionInput(in ApplyProjectionInput) (ApplyProjectionInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.ViewName = strings.TrimSpace(out.ViewName)
	out.AggregateType = strings.TrimSpace(out.AggregateType)
	out.AggregateID = strings.TrimSpace(out.AggregateID)
	out.EventID = strings.TrimSpace(out.EventID)

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.ViewName == "" {
		return out, errors.NewValidationError("view_name must not be empty")
	}
	if out.AggregateType == "" {
		return out, errors.NewValidationError("aggregate_type must not be empty")
	}
	if out.AggregateID == "" {
		return out, errors.NewValidationError("aggregate_id must not be empty")
	}
	if out.AggregateSeq <= 0 {
		return out, errors.NewValidationError("aggregate_seq must be strictly positive")
	}
	if out.EventID == "" {
		return out, errors.NewValidationError("event_id must not be empty")
	}
	if _, err := uuid.Parse(out.EventID); err != nil {
		return out, errors.NewValidationError("event_id must be a valid UUID")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}
	if out.OccurredAt.IsZero() {
		out.OccurredAt = time.Now().UTC()
	}

	return out, nil
}

func NormalizeInitiateRebuildInput(in InitiateRebuildInput) (InitiateRebuildInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.RebuildID = strings.TrimSpace(out.RebuildID)
	out.ViewName = strings.TrimSpace(out.ViewName)
	out.TargetShadowTable = strings.TrimSpace(out.TargetShadowTable)
	out.TargetLSN = strings.TrimSpace(out.TargetLSN)
	out.SourceStore = StorageTarget(strings.TrimSpace(string(out.SourceStore)))

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.SourceService == "" {
		return out, errors.NewValidationError("source_service must not be empty")
	}
	if out.ExternalID == "" {
		return out, errors.NewValidationError("external_id must not be empty")
	}
	if out.RebuildID == "" {
		return out, errors.NewValidationError("rebuild_id must not be empty")
	}
	if out.ViewName == "" {
		return out, errors.NewValidationError("view_name must not be empty")
	}
	if out.TargetShadowTable == "" {
		return out, errors.NewValidationError("target_shadow_table must not be empty")
	}
	if out.TargetLSN == "" {
		return out, errors.NewValidationError("target_lsn must not be empty")
	}
	if out.SourceStore == "" {
		out.SourceStore = StorageTargetPostgres
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	return out, nil
}

func NormalizeAdvanceRebuildInput(in AdvanceRebuildInput) (AdvanceRebuildInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.RebuildID = strings.TrimSpace(out.RebuildID)
	out.ExpectedPhase = RebuildPhase(strings.TrimSpace(string(out.ExpectedPhase)))
	out.TargetPhase = RebuildPhase(strings.TrimSpace(string(out.TargetPhase)))
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.RebuildID == "" {
		return out, errors.NewValidationError("rebuild_id must not be empty")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	return out, nil
}

