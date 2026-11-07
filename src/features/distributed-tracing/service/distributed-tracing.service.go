package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/distributed-tracing/contracts"
	"event-driven-platform/src/features/distributed-tracing/domain/machines"
	"event-driven-platform/src/features/distributed-tracing/domain/rules"
	"event-driven-platform/src/features/distributed-tracing/infrastructure/ports"
)

var (
	ErrCarrierMissing        = errors.New("carrier map cannot be nil")
	ErrInvalidCoverageReport = errors.New("invalid coverage report")
)

type DistributedTracingService struct {
	repo ports.DistributedTracingRepositoryPort
	sm   *machines.SamplingStateMachine
}

func NewDistributedTracingService(repo ports.DistributedTracingRepositoryPort) *DistributedTracingService {
	return &DistributedTracingService{
		repo: repo,
		sm:   machines.NewSamplingStateMachine(),
	}
}

func (s *DistributedTracingService) RegisterSamplingPolicy(
	ctx context.Context,
	policy contracts.TraceSamplingPolicy,
) (contracts.TraceSamplingPolicy, error) {
	if strings.TrimSpace(policy.ServiceName) == "" {
		return contracts.TraceSamplingPolicy{}, errors.New("service_name cannot be empty")
	}

	policy.ID = uuid.New()
	policy.PublicID = contracts.GeneratePublicID("tsp")
	policy.IsActive = true
	policy.CreatedAt = time.Now().UTC()
	policy.UpdatedAt = time.Now().UTC()

	if err := s.repo.SaveSamplingPolicy(ctx, policy); err != nil {
		return contracts.TraceSamplingPolicy{}, err
	}
	return policy, nil
}

func (s *DistributedTracingService) InjectTraceContext(
	ctx context.Context,
	cmd contracts.InjectTraceContextCommand,
) (contracts.InjectTraceContextResult, error) {
	if cmd.Carrier == nil {
		cmd.Carrier = make(map[string]string)
	}

	traceBytes := make([]byte, 16)
	spanBytes := make([]byte, 8)
	_, _ = rand.Read(traceBytes)
	_, _ = rand.Read(spanBytes)

	w3c := contracts.W3CTraceContext{
		TraceID:    hex.EncodeToString(traceBytes),
		SpanID:     hex.EncodeToString(spanBytes),
		TraceFlags: "01",
	}

	traceparent := rules.FormatW3CTraceparent(w3c)
	tracestate := "congo=t0"

	cmd.Carrier["traceparent"] = traceparent
	cmd.Carrier["tracestate"] = tracestate

	var corrUUID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	// The audit trail is best-effort relative to the injected carrier, but a
	// failure must be visible rather than silently dropped.
	if err := s.repo.RecordSpanAudit(ctx, contracts.TraceSpanAudit{
		ID:                      uuid.New(),
		PublicID:                contracts.GeneratePublicID("tsa"),
		TenantID:                cmd.TenantID,
		SourceService:           cmd.SourceService,
		ExternalID:              w3c.SpanID,
		CorrelationID:           corrUUID,
		TraceID:                 w3c.TraceID,
		SpanID:                  w3c.SpanID,
		ParentSpanID:            "",
		ServiceName:             cmd.SourceService,
		OperationName:           "InjectTraceContext",
		DurationMs:              1,
		HasError:                false,
		SanitizedAttributesJSON: rules.SanitizeAttributes(cmd.Carrier),
		CreatedAt:               time.Now().UTC(),
	}); err != nil {
		log.Printf("distributed-tracing: failed to record span audit for trace %s: %v", w3c.TraceID, err)
	}

	return contracts.InjectTraceContextResult{
		Traceparent: traceparent,
		Tracestate:  tracestate,
		Carrier:     cmd.Carrier,
	}, nil
}

func (s *DistributedTracingService) ExtractTraceContext(
	ctx context.Context,
	cmd contracts.ExtractTraceContextCommand,
) (contracts.ExtractTraceContextResult, error) {
	if cmd.Carrier == nil {
		return contracts.ExtractTraceContextResult{}, ErrCarrierMissing
	}

	rawHeader := cmd.Carrier["traceparent"]
	if rawHeader == "" {
		rawHeader = cmd.Carrier["Traceparent"]
	}

	w3c, err := rules.ParseW3CTraceparent(rawHeader)
	if err != nil {
		traceBytes := make([]byte, 16)
		spanBytes := make([]byte, 8)
		_, _ = rand.Read(traceBytes)
		_, _ = rand.Read(spanBytes)
		w3c = contracts.W3CTraceContext{
			TraceID:    hex.EncodeToString(traceBytes),
			SpanID:     hex.EncodeToString(spanBytes),
			TraceFlags: "01",
		}
	}

	mode, attachAsLink, attachAsChild := rules.ClassifyLinkingMode(cmd.IsBatch, cmd.QueueWaitMillis)

	return contracts.ExtractTraceContextResult{
		Context:       w3c,
		LinkingMode:   mode,
		AttachAsLink:  attachAsLink,
		AttachAsChild: attachAsChild,
	}, nil
}

func (s *DistributedTracingService) EvaluateSampling(
	ctx context.Context,
	cmd contracts.EvaluateSamplingCommand,
) (contracts.EvaluateSamplingResult, error) {
	policy, err := s.repo.GetSamplingPolicy(ctx, cmd.TenantID, cmd.SourceService)
	if err != nil {
		policy = contracts.TraceSamplingPolicy{
			TenantID:           cmd.TenantID,
			ServiceName:        cmd.SourceService,
			SampleRate:         1.0,
			AlwaysSampleErrors: true,
			IsActive:           true,
		}
	}

	res := s.sm.Evaluate(policy, cmd)
	return res, nil
}

func (s *DistributedTracingService) ReportCoverage(
	ctx context.Context,
	cmd contracts.ReportCoverageCommand,
) (contracts.ReportCoverageResult, error) {
	if err := validateCoverageCommand(&cmd); err != nil {
		return contracts.ReportCoverageResult{}, err
	}

	ratio := 1.0
	if cmd.TotalSpans > 0 {
		ratio = float64(cmd.PropagatedSpans) / float64(cmd.TotalSpans)
	}

	var corrUUID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	cov := contracts.TracePropagationCoverage{
		ID:              uuid.New(),
		PublicID:        contracts.GeneratePublicID("tpc"),
		TenantID:        cmd.TenantID,
		SourceService:   cmd.SourceService,
		ExternalID:      cmd.ExternalID,
		CorrelationID:   corrUUID,
		HopSource:       cmd.HopSource,
		HopTarget:       cmd.HopTarget,
		HopType:         cmd.HopType,
		TotalSpans:      cmd.TotalSpans,
		PropagatedSpans: cmd.PropagatedSpans,
		CoverageRatio:   ratio,
		EvaluatedAt:     time.Now().UTC(),
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}

	if err := s.repo.UpsertPropagationCoverage(ctx, cov); err != nil {
		return contracts.ReportCoverageResult{}, err
	}

	return contracts.ReportCoverageResult{
		HopSource:     cmd.HopSource,
		HopTarget:     cmd.HopTarget,
		CoverageRatio: ratio,
		IsHealthy:     ratio >= 0.95,
	}, nil
}

func validateCoverageCommand(cmd *contracts.ReportCoverageCommand) error {
	cmd.TenantID = strings.TrimSpace(cmd.TenantID)
	cmd.SourceService = strings.TrimSpace(cmd.SourceService)
	cmd.ExternalID = strings.TrimSpace(cmd.ExternalID)
	cmd.HopSource = strings.TrimSpace(cmd.HopSource)
	cmd.HopTarget = strings.TrimSpace(cmd.HopTarget)

	for name, v := range map[string]string{
		"tenant_id":      cmd.TenantID,
		"source_service": cmd.SourceService,
		"external_id":    cmd.ExternalID,
		"hop_source":     cmd.HopSource,
		"hop_target":     cmd.HopTarget,
	} {
		if v == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidCoverageReport, name)
		}
	}
	switch cmd.HopType {
	case contracts.HopTypeConnectRPC, contracts.HopTypeOutboxRelay, contracts.HopTypeKafkaConsumer, contracts.HopTypeAsyncReaction:
	default:
		return fmt.Errorf("%w: hop_type must be one of CONNECT_RPC, OUTBOX_RELAY, KAFKA_CONSUMER, ASYNC_REACTION", ErrInvalidCoverageReport)
	}
	if cmd.TotalSpans < 0 || cmd.PropagatedSpans < 0 {
		return fmt.Errorf("%w: span counts must be non-negative", ErrInvalidCoverageReport)
	}
	if cmd.PropagatedSpans > cmd.TotalSpans {
		return fmt.Errorf("%w: propagated_spans cannot exceed total_spans", ErrInvalidCoverageReport)
	}
	return nil
}
