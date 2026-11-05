package service

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/domain/machines"
	"event-driven-platform/src/features/backpressure/domain/rules"
	"event-driven-platform/src/features/backpressure/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/utils"
)

type BackpressureService struct {
	repo ports.BackpressureRepositoryPort
}

func NewBackpressureService(repo ports.BackpressureRepositoryPort) *BackpressureService {
	return &BackpressureService{repo: repo}
}

func (s *BackpressureService) ReportSaturation(ctx context.Context, input contracts.ReportSaturationInput) (*contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.report_saturation")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.Signal.HandlerID),
		attribute.String("dependency_id", input.Signal.DependencyID),
		attribute.String("pause_reason", string(input.PauseReason)),
	)

	if err := contracts.ValidateReportSaturationInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	eval := rules.EvaluateSaturation(input.Signal)
	if !eval.IsSaturated {
		return nil, fmt.Errorf("signal does not indicate saturation: %s", eval.Recommendation)
	}

	existing, getErr := s.repo.GetBackpressureRecord(ctx, input.TenantID, input.Signal.HandlerID, input.Signal.DependencyID)

	fromState := contracts.StateNormal
	currentLimit := int64(10)
	if getErr == nil && existing != nil {
		fromState = existing.State
		currentLimit = existing.ConcurrencyLimit
	}

	nextState, transErr := machines.TransitionTo(fromState, contracts.EventSaturationDetected)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	newLimit := rules.ComputeDecreasedConcurrency(currentLimit)
	now := time.Now().UTC()

	recordID, err := utils.GenerateUUIDv7()
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	publicID, err := utils.GeneratePublicID("bp")
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	rec := contracts.BackpressureRecord{
		ID:               recordID,
		PublicID:         publicID,
		TenantID:         input.TenantID,
		SourceService:    input.SourceService,
		ExternalID:       input.ExternalID,
		CorrelationID:    input.CorrelationID,
		HandlerID:        input.Signal.HandlerID,
		DependencyID:     input.Signal.DependencyID,
		State:            nextState,
		PauseReason:      input.PauseReason,
		ConcurrencyLimit: newLimit,
		PausedAt:         &now,
	}

	saved, saveErr := s.repo.UpsertBackpressureRecord(ctx, rec)
	if saveErr != nil {
		observability.RecordError(span, saveErr, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, saveErr
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("bpa")
	audit := contracts.BackpressureAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    fmt.Sprintf("%s-sat-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID: input.CorrelationID,
		HandlerID:     input.Signal.HandlerID,
		DependencyID:  input.Signal.DependencyID,
		FromState:     fromState,
		ToState:       nextState,
		Event:         contracts.EventSaturationDetected,
		PauseReason:   input.PauseReason,
		Reason:        string(eval.SaturationReason),
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(
		attribute.String("bp.public_id", saved.PublicID),
		attribute.String("bp.state", string(nextState)),
		attribute.Int64("bp.concurrency_limit", newLimit),
	)

	return saved, nil
}

func (s *BackpressureService) ReportRecovery(ctx context.Context, input contracts.ReportRecoveryInput) (*contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.report_recovery")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("dependency_id", input.DependencyID),
	)

	if err := contracts.ValidateReportRecoveryInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	existing, getErr := s.repo.GetBackpressureRecord(ctx, input.TenantID, input.HandlerID, input.DependencyID)
	if getErr != nil {
		observability.RecordError(span, getErr, attribute.String("error.type", "NOT_FOUND"))
		return nil, getErr
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventRecoveryStarted)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	rampLimit := rules.ComputeRampConcurrency(existing.ConcurrencyLimit)
	now := time.Now().UTC()

	rec := *existing
	rec.State = nextState
	rec.ConcurrencyLimit = rampLimit
	rec.ResumedAt = &now
	rec.PauseReason = ""

	saved, saveErr := s.repo.UpsertBackpressureRecord(ctx, rec)
	if saveErr != nil {
		observability.RecordError(span, saveErr, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, saveErr
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("bpa")
	audit := contracts.BackpressureAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    fmt.Sprintf("%s-rec-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID: input.CorrelationID,
		HandlerID:     input.HandlerID,
		DependencyID:  input.DependencyID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventRecoveryStarted,
		Reason:        "recovery-ramp-started",
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(
		attribute.String("bp.state", string(nextState)),
		attribute.Int64("bp.ramp_limit", rampLimit),
	)

	return saved, nil
}

func (s *BackpressureService) ConfirmRecovery(ctx context.Context, input contracts.ReportRecoveryInput) (*contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.confirm_recovery")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("dependency_id", input.DependencyID),
	)

	if err := contracts.ValidateReportRecoveryInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	existing, getErr := s.repo.GetBackpressureRecord(ctx, input.TenantID, input.HandlerID, input.DependencyID)
	if getErr != nil {
		observability.RecordError(span, getErr, attribute.String("error.type", "NOT_FOUND"))
		return nil, getErr
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventRecoveryConfirmed)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	rec := *existing
	rec.State = nextState
	rec.PauseReason = ""

	saved, saveErr := s.repo.UpsertBackpressureRecord(ctx, rec)
	if saveErr != nil {
		observability.RecordError(span, saveErr, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, saveErr
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("bpa")
	audit := contracts.BackpressureAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    fmt.Sprintf("%s-conf-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID: input.CorrelationID,
		HandlerID:     input.HandlerID,
		DependencyID:  input.DependencyID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventRecoveryConfirmed,
		Reason:        "recovery-confirmed-normal",
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(attribute.String("bp.state", string(nextState)))

	return saved, nil
}

func (s *BackpressureService) GetState(ctx context.Context, input contracts.GetBackpressureStateInput) (*contracts.BackpressureStateResult, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.get_state")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("dependency_id", input.DependencyID),
	)

	if err := contracts.ValidateGetBackpressureStateInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	rec, getErr := s.repo.GetBackpressureRecord(ctx, input.TenantID, input.HandlerID, input.DependencyID)
	if getErr != nil {
		observability.RecordError(span, getErr, attribute.String("error.type", "NOT_FOUND"))
		return nil, getErr
	}

	result := &contracts.BackpressureStateResult{
		HandlerID:        rec.HandlerID,
		DependencyID:     rec.DependencyID,
		State:            rec.State,
		ConcurrencyLimit: rec.ConcurrencyLimit,
		PauseReason:      rec.PauseReason,
		PausedAt:         rec.PausedAt,
	}

	if rec.PausedAt != nil {
		result.PauseDurationMs = time.Since(*rec.PausedAt).Milliseconds()
	}

	span.SetAttributes(
		attribute.String("bp.state", string(rec.State)),
		attribute.Int64("bp.concurrency_limit", rec.ConcurrencyLimit),
	)

	return result, nil
}

func (s *BackpressureService) ListByTenant(ctx context.Context, tenantID string) ([]contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.list_by_tenant")
	defer span.End()

	span.SetAttributes(attribute.String("tenant_id", tenantID))

	if tenantID == "" {
		return nil, contracts.ErrMissingTenantID
	}

	recs, err := s.repo.ListBackpressureByTenant(ctx, tenantID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	span.SetAttributes(attribute.Int("bp.count", len(recs)))
	return recs, nil
}

