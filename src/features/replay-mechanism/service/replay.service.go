package service

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/features/replay-mechanism/domain/machines"
	"event-driven-platform/src/features/replay-mechanism/domain/rules"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/utils"
)

type ReplayService struct {
	repo ports.ReplayRepositoryPort
}

func NewReplayService(repo ports.ReplayRepositoryPort) *ReplayService {
	return &ReplayService{repo: repo}
}

func (s *ReplayService) CreateReplayRun(ctx context.Context, input contracts.CreateReplayRunInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.create_run")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("target_projection", input.TargetProjection),
		attribute.String("mode", string(input.Mode)),
		attribute.String("scope", string(input.Scope)),
	)

	if err := contracts.ValidateCreateReplayRunInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	recordID, err := utils.GenerateUUIDv7()
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	publicID, err := utils.GeneratePublicID("rpl")
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	now := time.Now().UTC()
	consumerGroup := rules.GenerateReplayConsumerGroup(input.TargetProjection, now)

	rec := contracts.ReplayRunRecord{
		ID:                  recordID,
		PublicID:            publicID,
		TenantID:            input.TenantID,
		SourceService:       input.SourceService,
		ExternalID:          input.ExternalID,
		CorrelationID:       input.CorrelationID,
		TargetProjection:    input.TargetProjection,
		Mode:                input.Mode,
		State:               contracts.StateInitializing,
		Scope:               input.Scope,
		StartOffset:         input.StartOffset,
		CurrentOffset:       input.StartOffset,
		EndOffset:           input.EndOffset,
		ReplayConsumerGroup: consumerGroup,
		Version:             1,
	}

	saved, saveErr := s.repo.CreateReplayRun(ctx, rec)
	if saveErr != nil {
		observability.RecordError(span, saveErr, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, saveErr
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("rpa")
	audit := contracts.ReplayAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    fmt.Sprintf("%s-init-%d", input.ExternalID, now.UnixNano()),
		CorrelationID: input.CorrelationID,
		RunID:         saved.ID,
		FromState:     contracts.StateInitializing,
		ToState:       contracts.StateInitializing,
		Event:         contracts.EventStartStreaming,
		Reason:        "replay-run-initialized",
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(
		attribute.String("replay.public_id", saved.PublicID),
		attribute.String("replay.consumer_group", consumerGroup),
	)

	return saved, nil
}

func (s *ReplayService) StartStreaming(ctx context.Context, tenantID, runID string) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.start_streaming")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("run_id", runID),
	)

	existing, err := s.repo.GetReplayRun(ctx, tenantID, runID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventStartStreaming)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	updated, err := s.repo.UpdateReplayRunState(ctx, tenantID, runID, nextState)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("rpa")
	now := time.Now().UTC()
	audit := contracts.ReplayAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      tenantID,
		SourceService: existing.SourceService,
		ExternalID:    fmt.Sprintf("%s-start-%d", existing.ExternalID, now.UnixNano()),
		CorrelationID: existing.CorrelationID,
		RunID:         runID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventStartStreaming,
		Reason:        "historical-streaming-started",
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(attribute.String("replay.state", string(nextState)))
	return updated, nil
}

func (s *ReplayService) AdvanceReplay(ctx context.Context, input contracts.AdvanceReplayInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.advance_replay")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
		attribute.Int64("current_offset", input.CurrentOffset),
	)

	if err := contracts.ValidateAdvanceReplayInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	existing, err := s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	if err := rules.ValidateOffsetAdvance(existing.CurrentOffset, input.CurrentOffset, existing.EndOffset); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	targetState := existing.State
	if rules.IsCaughtUp(input.CurrentOffset, existing.EndOffset) && existing.State == contracts.StateStreaming {
		if next, err := machines.TransitionTo(existing.State, contracts.EventCatchUpReached); err == nil {
			targetState = next
		}
	}

	updated, err := s.repo.UpdateReplayRunOffset(ctx, input.TenantID, input.RunID, input.CurrentOffset, targetState)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	span.SetAttributes(
		attribute.String("replay.state", string(updated.State)),
		attribute.Int64("replay.offset", updated.CurrentOffset),
	)
	return updated, nil
}

func (s *ReplayService) PromoteReplay(ctx context.Context, input contracts.PromoteReplayInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.promote_replay")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
	)

	if err := contracts.ValidatePromoteReplayInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	existing, err := s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventPromoteWriter)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	updated, err := s.repo.UpdateReplayRunState(ctx, input.TenantID, input.RunID, nextState)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("rpa")
	now := time.Now().UTC()
	reason := input.Reason
	if reason == "" {
		reason = "replay-promoted-to-live-writer"
	}
	audit := contracts.ReplayAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: existing.SourceService,
		ExternalID:    fmt.Sprintf("%s-prom-%d", existing.ExternalID, now.UnixNano()),
		CorrelationID: existing.CorrelationID,
		RunID:         input.RunID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventPromoteWriter,
		Reason:        reason,
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(attribute.String("replay.state", string(nextState)))
	return updated, nil
}

func (s *ReplayService) CompleteCutover(ctx context.Context, input contracts.CompleteCutoverInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.complete_cutover")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
	)

	if err := contracts.ValidateCompleteCutoverInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	existing, err := s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventCompleteCutover)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	updated, err := s.repo.UpdateReplayRunState(ctx, input.TenantID, input.RunID, nextState)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("rpa")
	now := time.Now().UTC()
	reason := input.Reason
	if reason == "" {
		reason = "cutover-completed-successfully"
	}
	audit := contracts.ReplayAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: existing.SourceService,
		ExternalID:    fmt.Sprintf("%s-cut-%d", existing.ExternalID, now.UnixNano()),
		CorrelationID: existing.CorrelationID,
		RunID:         input.RunID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventCompleteCutover,
		Reason:        reason,
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(attribute.String("replay.state", string(nextState)))
	return updated, nil
}

func (s *ReplayService) AbortReplay(ctx context.Context, input contracts.AbortReplayInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.abort_replay")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
	)

	existing, err := s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	nextState, transErr := machines.TransitionTo(existing.State, contracts.EventAbortRun)
	if transErr != nil {
		observability.RecordError(span, transErr, attribute.String("error.type", "INVALID_TRANSITION"))
		return nil, transErr
	}

	updated, err := s.repo.UpdateReplayRunState(ctx, input.TenantID, input.RunID, nextState)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("rpa")
	now := time.Now().UTC()
	reason := input.Reason
	if reason == "" {
		reason = "replay-run-aborted"
	}
	audit := contracts.ReplayAuditRecord{
		ID:            auditID,
		PublicID:      auditPublicID,
		TenantID:      input.TenantID,
		SourceService: existing.SourceService,
		ExternalID:    fmt.Sprintf("%s-abort-%d", existing.ExternalID, now.UnixNano()),
		CorrelationID: existing.CorrelationID,
		RunID:         input.RunID,
		FromState:     existing.State,
		ToState:       nextState,
		Event:         contracts.EventAbortRun,
		Reason:        reason,
	}
	_ = s.repo.RecordAuditLog(ctx, audit)

	span.SetAttributes(attribute.String("replay.state", string(nextState)))
	return updated, nil
}

func (s *ReplayService) EvaluateEffect(ctx context.Context, input contracts.EvaluateEffectInput) (*contracts.EffectEvaluationResult, error) {
	ctx, span := observability.StartSpan(ctx, "replay.evaluate_effect")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
		attribute.String("effect_type", string(input.EffectType)),
	)

	if err := contracts.ValidateEvaluateEffectInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	run, err := s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	mode := rules.ResolveEffectMode(run.Mode, input.EffectType)
	decision := rules.EvaluateEffectDecision(mode)

	effID, _ := utils.GenerateUUIDv7()
	effPublicID, _ := utils.GeneratePublicID("rpe")
	effRecord := contracts.ReplayEffectRecord{
		ID:             effID,
		PublicID:       effPublicID,
		TenantID:       input.TenantID,
		SourceService:  input.SourceService,
		ExternalID:     input.ExternalID,
		CorrelationID:  input.CorrelationID,
		RunID:          input.RunID,
		EffectType:     input.EffectType,
		EffectMode:     mode,
		Decision:       decision,
		IdempotencyKey: input.IdempotencyKey,
		PayloadDigest:  input.PayloadDigest,
	}
	_ = s.repo.RecordEffectLog(ctx, effRecord)

	reason := fmt.Sprintf("effect %s resolved for run mode %s", decision, run.Mode)
	return &contracts.EffectEvaluationResult{
		Decision:   decision,
		EffectMode: mode,
		Reason:     reason,
	}, nil
}

func (s *ReplayService) GetReplayRun(ctx context.Context, input contracts.GetReplayRunInput) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.get_run")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("run_id", input.RunID),
	)

	return s.repo.GetReplayRun(ctx, input.TenantID, input.RunID)
}

func (s *ReplayService) ListReplayRuns(ctx context.Context, tenantID string) ([]contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.list_runs")
	defer span.End()

	span.SetAttributes(attribute.String("tenant_id", tenantID))
	return s.repo.ListReplayRunsByTenant(ctx, tenantID)
}

