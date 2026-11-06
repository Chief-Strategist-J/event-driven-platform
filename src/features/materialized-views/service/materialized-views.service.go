package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/domain/machines"
	"event-driven-platform/src/features/materialized-views/domain/rules"
	"event-driven-platform/src/features/materialized-views/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type ViewService struct {
	repo    ports.ViewRepositoryPort
	machine *machines.ViewRebuildStateMachine
	tracer  trace.Tracer
}

func NewViewService(repo ports.ViewRepositoryPort) *ViewService {
	return &ViewService{
		repo:    repo,
		machine: machines.NewViewRebuildStateMachine(),
		tracer:  otel.Tracer("ViewService"),
	}
}

func (s *ViewService) RegisterView(ctx context.Context, in contracts.RegisterViewInput) (*contracts.ViewRegistryRecord, error) {
	ctx, span := s.tracer.Start(ctx, "ViewService.RegisterView")
	defer span.End()

	normalized, err := contracts.NormalizeRegisterViewInput(in)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	corrID, _ := uuid.Parse(normalized.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("mvr_%s", uuid.New().String()[:20])

	record := contracts.ViewRegistryRecord{
		ID:              uuid.New(),
		PublicID:        pubID,
		TenantID:        normalized.TenantID,
		SourceService:   normalized.SourceService,
		ExternalID:      normalized.ExternalID,
		CorrelationID:   corrID,
		ViewName:        normalized.ViewName,
		ViewClass:       normalized.ViewClass,
		PrimaryStore:    normalized.PrimaryStore,
		ActiveTableName: normalized.ActiveTableName,
		FreshnessSLOMs:  normalized.FreshnessSLOMs,
		ConsistencyMode: normalized.ConsistencyMode,
		Status:          contracts.ViewStatusOnline,
		Version:         1,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := rules.ValidateViewRegistration(record); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if err := s.repo.RegisterView(ctx, record); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return &record, nil
}

func (s *ViewService) GetViewByPublicID(ctx context.Context, publicID string) (*contracts.ViewRegistryRecord, error) {
	ctx, span := s.tracer.Start(ctx, "ViewService.GetViewByPublicID")
	defer span.End()

	if publicID == "" {
		err := errors.NewValidationError("public_id must not be empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	record, err := s.repo.GetViewByPublicID(ctx, publicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return record, nil
}

func (s *ViewService) ApplyProjection(ctx context.Context, in contracts.ApplyProjectionInput) (*contracts.ProjectionEvaluation, error) {
	ctx, span := s.tracer.Start(ctx, "ViewService.ApplyProjection")
	defer span.End()

	norm, err := contracts.NormalizeApplyProjectionInput(in)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	currentState, err := s.repo.GetProjectionState(ctx, norm.TenantID, norm.ViewName, norm.AggregateType, norm.AggregateID)
	var currentVersion int64
	var lastHash string
	if err == nil && currentState != nil {
		currentVersion = currentState.CurrentVersion
		lastHash = currentState.PayloadHash
	}

	evaluation, err := rules.EvaluateProjectionVersion(currentVersion, lastHash, norm)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	switch evaluation.Decision {
	case contracts.DecisionApply:
		eventUUID, _ := uuid.Parse(norm.EventID)
		corrUUID, _ := uuid.Parse(norm.CorrelationID)
		now := time.Now().UTC()
		pubID := fmt.Sprintf("mps_%s", uuid.New().String()[:20])

		state := contracts.ProjectionStateRecord{
			ID:             uuid.New(),
			PublicID:       pubID,
			TenantID:       norm.TenantID,
			SourceService:  norm.SourceService,
			ExternalID:     norm.ExternalID,
			CorrelationID:  corrUUID,
			ViewName:       norm.ViewName,
			AggregateType:  norm.AggregateType,
			AggregateID:    norm.AggregateID,
			CurrentVersion: norm.AggregateSeq,
			LastEventID:    eventUUID,
			PayloadHash:    evaluation.PayloadHash,
			MaterializedAt: now,
			Version:        1,
			CreatedAt:      now,
			UpdatedAt:      now,
		}

		if err := s.repo.UpsertProjectionState(ctx, state); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

	case contracts.DecisionGapHold:
		corrUUID, _ := uuid.Parse(norm.CorrelationID)
		pubID := fmt.Sprintf("mvh_%s", uuid.New().String()[:20])

		held := contracts.HeldEventRecord{
			ID:            uuid.New(),
			PublicID:      pubID,
			TenantID:      norm.TenantID,
			SourceService: norm.SourceService,
			ExternalID:    norm.ExternalID,
			CorrelationID: corrUUID,
			ViewName:      norm.ViewName,
			AggregateType: norm.AggregateType,
			AggregateID:   norm.AggregateID,
			ExpectedSeq:   evaluation.ExpectedSeq,
			ReceivedSeq:   evaluation.IncomingSeq,
			EventPayload:  norm.PayloadRaw,
			HeldUntil:     time.Now().UTC().Add(10 * time.Minute),
			CreatedAt:     time.Now().UTC(),
		}

		if err := s.repo.QuarantineHeldEvent(ctx, held); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

	case contracts.DecisionDuplicate, contracts.DecisionStaleDrop:
	}

	return &evaluation, nil
}

func (s *ViewService) InitiateRebuild(ctx context.Context, in contracts.InitiateRebuildInput) (*contracts.RebuildLogRecord, error) {
	ctx, span := s.tracer.Start(ctx, "ViewService.InitiateRebuild")
	defer span.End()

	norm, err := contracts.NormalizeInitiateRebuildInput(in)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("mvr_reb_%s", uuid.New().String()[:16])

	logRecord := contracts.RebuildLogRecord{
		ID:                uuid.New(),
		PublicID:          pubID,
		TenantID:          norm.TenantID,
		SourceService:     norm.SourceService,
		ExternalID:        norm.ExternalID,
		CorrelationID:     corrUUID,
		RebuildID:         norm.RebuildID,
		ViewName:          norm.ViewName,
		SourceStore:       norm.SourceStore,
		TargetShadowTable: norm.TargetShadowTable,
		TargetLSN:         norm.TargetLSN,
		CurrentCursorSeq:  0,
		Phase:             contracts.RebuildPhaseInitiated,
		Version:           1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := s.repo.InitiateRebuild(ctx, logRecord); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if err := s.repo.UpdateViewStatus(ctx, norm.TenantID, norm.ViewName, contracts.ViewStatusRebuilding); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return &logRecord, nil
}

func (s *ViewService) AdvanceRebuild(ctx context.Context, in contracts.AdvanceRebuildInput) (*contracts.RebuildLogRecord, error) {
	ctx, span := s.tracer.Start(ctx, "ViewService.AdvanceRebuild")
	defer span.End()

	norm, err := contracts.NormalizeAdvanceRebuildInput(in)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log, err := s.repo.GetRebuildLog(ctx, norm.TenantID, norm.RebuildID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if log.Phase != norm.ExpectedPhase {
		err := errors.NewConflictError(fmt.Sprintf("rebuild phase mismatch: expected %s, found %s", norm.ExpectedPhase, log.Phase))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	nextPhase, err := s.machine.Transition(log.Phase, norm.TargetPhase)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if err := s.repo.UpdateRebuildPhase(ctx, norm.TenantID, norm.RebuildID, nextPhase, log.CurrentCursorSeq); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if nextPhase == contracts.RebuildPhasePromoted {
		if err := s.repo.UpdateViewStatus(ctx, norm.TenantID, log.ViewName, contracts.ViewStatusOnline); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}

	log.Phase = nextPhase
	return log, nil
}
