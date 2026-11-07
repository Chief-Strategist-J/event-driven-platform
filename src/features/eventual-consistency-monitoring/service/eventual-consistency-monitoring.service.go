package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/domain/machines"
	"event-driven-platform/src/features/eventual-consistency-monitoring/domain/rules"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type ConsistencyService struct {
	repo   ports.ConsistencyRepositoryPort
	tracer trace.Tracer
}

func NewConsistencyService(repo ports.ConsistencyRepositoryPort) *ConsistencyService {
	return &ConsistencyService{
		repo:   repo,
		tracer: otel.Tracer("ConsistencyService"),
	}
}

func (s *ConsistencyService) ObserveWatermark(ctx context.Context, in contracts.ObserveWatermarkInput) (*contracts.WatermarkRecord, error) {
	ctx, span := s.tracer.Start(ctx, "ConsistencyService.ObserveWatermark")
	defer span.End()

	norm, err := contracts.NormalizeObserveWatermarkInput(in)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("ecw_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	offsetLag := norm.HighWatermarkOffset - norm.ConsumerOffset
	if offsetLag < 0 {
		offsetLag = 0
	}

	watermarkStatus := rules.CalculateWatermarkStatus(norm.FreshnessLagMs, 1000, offsetLag)

	record := contracts.WatermarkRecord{
		ID:                  uuid.New(),
		PublicID:            pubID,
		TenantID:            norm.TenantID,
		SourceService:       norm.SourceService,
		ExternalID:          norm.ExternalID,
		CorrelationID:       corrUUID,
		ViewName:            norm.ViewName,
		StreamName:          norm.StreamName,
		PartitionID:         norm.PartitionID,
		HighWatermarkOffset: norm.HighWatermarkOffset,
		ConsumerOffset:      norm.ConsumerOffset,
		OffsetLag:           offsetLag,
		FreshnessLagMs:      norm.FreshnessLagMs,
		WatermarkStatus:     watermarkStatus,
		ObservedAt:          now,
		Version:             1,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := s.repo.UpsertWatermark(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *ConsistencyService) GetViewWatermarks(ctx context.Context, tenantID string, viewName string) ([]contracts.WatermarkRecord, error) {
	return s.repo.GetWatermarksByView(ctx, tenantID, viewName)
}

func (s *ConsistencyService) EvaluateAndRecordDrift(
	ctx context.Context,
	in contracts.EvaluateRecordInput,
	sourceService string,
	correlationID string,
) (*contracts.EvaluationResult, error) {
	result := rules.EvaluateThreeValuedConsistency(in)
	if result.Outcome != contracts.OutcomeMismatch {
		return &result, nil
	}

	corrUUID, err := uuid.Parse(correlationID)
	if err != nil {
		corrUUID = uuid.New()
	}

	now := time.Now().UTC()
	pubID := fmt.Sprintf("ecd_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	incident := contracts.DriftIncidentRecord{
		ID:                uuid.New(),
		PublicID:          pubID,
		TenantID:          in.TenantID,
		SourceService:     sourceService,
		ExternalID:        uuid.New().String(),
		CorrelationID:     corrUUID,
		ViewName:          in.ViewName,
		AggregateType:     in.AggregateType,
		AggregateID:       in.AggregateID,
		DriftClass:        result.DriftClass,
		SourceVersion:     in.SourceVersion,
		ViewVersion:       in.ViewVersion,
		DifferencePayload: result.DifferencePayload,
		Status:            contracts.IncidentDetected,
		HealAttempts:      0,
		QuarantinedUntil:  now.Add(2 * time.Minute),
		DetectedAt:        now,
		ResolvedAt:        now,
		Version:           1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := s.repo.RecordDriftIncident(ctx, incident); err != nil {
		return nil, err
	}

	return &result, nil
}

func (s *ConsistencyService) GetActiveIncidents(ctx context.Context, tenantID string, viewName string) ([]contracts.DriftIncidentRecord, error) {
	return s.repo.GetActiveDriftIncidents(ctx, tenantID, viewName)
}

// AttemptHealing loads the incident (returning ports.ErrIncidentNotFound when it
// does not exist) and applies the repeat-heal policy using the persisted
// heal_attempts counter.
func (s *ConsistencyService) AttemptHealing(ctx context.Context, incidentID uuid.UUID, tenantID string, maxAttempts int) (bool, error) {
	incident, err := s.repo.GetDriftIncident(ctx, incidentID, tenantID)
	if err != nil {
		return false, err
	}

	allowed := rules.EvaluateRepeatHeal(incident.HealAttempts, maxAttempts)
	if !allowed {
		if err := s.repo.UpdateDriftIncidentStatus(ctx, incidentID, tenantID, contracts.IncidentManualIntervention, incident.HealAttempts); err != nil {
			return false, err
		}
		return false, nil
	}

	attempts := incident.HealAttempts + 1
	nextState := machines.EvaluateIncidentLifecycle(contracts.IncidentConfirmed, 5*time.Minute, 2*time.Minute, true, attempts)
	if err := s.repo.UpdateDriftIncidentStatus(ctx, incidentID, tenantID, nextState, attempts); err != nil {
		return false, err
	}
	return true, nil
}

func (s *ConsistencyService) RecordCheckSweep(ctx context.Context, in contracts.RunCheckSweepInput) (*contracts.CheckRunRecord, error) {
	norm, err := contracts.NormalizeRunCheckSweepInput(in)
	if err != nil {
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("ecc_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	ratio := rules.CalculateCoverageRatio(norm.MatchCount, norm.MismatchCount, norm.UnknownCount)

	status := "COMPLETED"
	if norm.UnknownCount > 0 && float64(norm.UnknownCount)/float64(norm.EvaluatedKeys+1) > 0.05 {
		status = "DEGRADED"
	}

	record := contracts.CheckRunRecord{
		ID:                  uuid.New(),
		PublicID:            pubID,
		TenantID:            norm.TenantID,
		SourceService:       norm.SourceService,
		ExternalID:          uuid.New().String(),
		CorrelationID:       corrUUID,
		ViewName:            norm.ViewName,
		CheckType:           norm.CheckType,
		EvaluatedKeys:       norm.EvaluatedKeys,
		MatchCount:          norm.MatchCount,
		MismatchCount:       norm.MismatchCount,
		UnknownCount:        norm.UnknownCount,
		CoverageRatio:       ratio,
		ExecutionDurationMs: norm.DurationMs,
		Status:              status,
		CreatedAt:           now,
	}

	if err := s.repo.RecordCheckRun(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *ConsistencyService) ListCheckRuns(ctx context.Context, tenantID string, viewName string, limit int) ([]contracts.CheckRunRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	return s.repo.ListCheckRuns(ctx, tenantID, viewName, limit)
}

func (s *ConsistencyService) RegisterSilence(ctx context.Context, in contracts.RegisterSilenceInput) (*contracts.SilenceRecord, error) {
	norm, err := contracts.NormalizeRegisterSilenceInput(in)
	if err != nil {
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("ecs_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	record := contracts.SilenceRecord{
		ID:            uuid.New(),
		PublicID:      pubID,
		TenantID:      norm.TenantID,
		SourceService: norm.SourceService,
		ExternalID:    norm.ExternalID,
		CorrelationID: corrUUID,
		ViewName:      norm.ViewName,
		OwnerID:       norm.OwnerID,
		Reason:        norm.Reason,
		ExpiresAt:     norm.ExpiresAt,
		Status:        contracts.SilenceActive,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.repo.RegisterSilence(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

// DispatchCanaryProbe records the probe in consistency_check_run_history
// (check_type CANARY_PROBE). This service does not publish the canary event or
// observe it downstream, so no end-to-end evidence of delivery exists at this
// point: the outcome is UNKNOWN (recorded as one unknown key, coverage 0) rather
// than MATCH. A later reconciliation step must confirm arrival.
func (s *ConsistencyService) DispatchCanaryProbe(ctx context.Context, in contracts.CanaryProbeInput) (*contracts.CanaryProbeOutput, error) {
	norm, err := contracts.NormalizeCanaryProbeInput(in)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	probeUUID := uuid.New()
	probeID := fmt.Sprintf("probe_%s", strings.ReplaceAll(probeUUID.String(), "-", "")[:16])

	corrUUID, err := uuid.Parse(norm.CorrelationID)
	if err != nil {
		corrUUID = uuid.New()
	}

	record := contracts.CheckRunRecord{
		ID:                  probeUUID,
		PublicID:            fmt.Sprintf("ecc_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20]),
		TenantID:            norm.TenantID,
		SourceService:       norm.SourceService,
		ExternalID:          probeID,
		CorrelationID:       corrUUID,
		ViewName:            norm.StreamName,
		CheckType:           contracts.CheckCanaryProbe,
		EvaluatedKeys:       1,
		UnknownCount:        1,
		CoverageRatio:       rules.CalculateCoverageRatio(0, 0, 1),
		ExecutionDurationMs: time.Since(start).Milliseconds(),
		Status:              "COMPLETED",
		CreatedAt:           time.Now().UTC(),
	}
	if err := s.repo.RecordCheckRun(ctx, record); err != nil {
		return nil, err
	}

	return &contracts.CanaryProbeOutput{
		ProbeID:           probeID,
		PartitionID:       norm.PartitionID,
		TransitDurationMs: record.ExecutionDurationMs,
		Outcome:           contracts.OutcomeUnknown,
	}, nil
}
