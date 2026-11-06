package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
	"event-driven-platform/src/features/event-operations-dashboard/domain/rules"
	"event-driven-platform/src/features/event-operations-dashboard/infrastructure/ports"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type EventOperationsDashboardService struct {
	repo   ports.EventOperationsDashboardRepositoryPort
	tracer trace.Tracer
}

func NewEventOperationsDashboardService(repo ports.EventOperationsDashboardRepositoryPort) *EventOperationsDashboardService {
	return &EventOperationsDashboardService{
		repo:   repo,
		tracer: otel.Tracer("EventOperationsDashboardService"),
	}
}

func (s *EventOperationsDashboardService) ReportHopTelemetry(ctx context.Context, input *contracts.ReportHopTelemetryInput) (*contracts.PipelineHopRecord, error) {
	ctx, span := s.tracer.Start(ctx, "EventOperationsDashboardService.ReportHopTelemetry",
		trace.WithAttributes(
			attribute.String("ops.tenant_id", input.TenantID),
			attribute.String("ops.flow_name", input.FlowName),
			attribute.String("ops.hop_code", string(input.HopCode)),
			attribute.String("ops.status", string(input.Status)),
		),
	)
	defer span.End()

	record, err := contracts.NormalizeReportHopTelemetryInput(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if err := s.repo.SaveHopTelemetry(ctx, record); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to save hop telemetry: %w", err)
	}

	span.SetAttributes(attribute.String("ops.hop_public_id", record.PublicID))
	span.SetStatus(codes.Ok, "hop telemetry reported")
	return record, nil
}

func (s *EventOperationsDashboardService) TriageFlow(ctx context.Context, input *contracts.TriageFlowInput) (*contracts.TriageFlowOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EventOperationsDashboardService.TriageFlow",
		trace.WithAttributes(
			attribute.String("ops.tenant_id", input.TenantID),
			attribute.String("ops.flow_name", input.FlowName),
			attribute.String("ops.incident_id", input.IncidentID),
		),
	)
	defer span.End()

	norm, err := contracts.NormalizeTriageFlowInput(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	hops, err := s.repo.GetHopsByFlow(ctx, norm.TenantID, norm.FlowName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	now := time.Now().UTC()
	brokenHop, status, sev, action, cctl := rules.LocalizeBrokenHop(hops, now)

	localizedCode := contracts.HopCode("NONE")
	pReason := contracts.PauseReasonNone
	runbook := "https://wiki.platform.local/ops/overview"
	if brokenHop != nil {
		localizedCode = brokenHop.HopCode
		pReason = brokenHop.PauseReason
		runbook = brokenHop.RunbookURL
	}

	triagePubID, _ := contracts.GeneratePublicID("itl")
	triageLog := &contracts.IncidentTriageRecord{
		ID:             uuid.New(),
		PublicID:       triagePubID,
		TenantID:       norm.TenantID,
		SourceService:  norm.SourceService,
		ExternalID:     norm.ExternalID,
		CorrelationID:  norm.CorrelationID,
		IncidentID:     norm.IncidentID,
		FlowName:       norm.FlowName,
		LocalizedHop:   string(localizedCode),
		Severity:       sev,
		TriageAction:   action,
		CCTLActionLink: cctl,
		TriagedBy:      norm.TriagedBy,
		TriagedAt:      now,
	}

	if err := s.repo.SaveTriageLog(ctx, triageLog); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to persist triage log: %w", err)
	}

	span.SetAttributes(
		attribute.String("ops.localized_hop", string(localizedCode)),
		attribute.String("ops.severity", string(sev)),
	)
	span.SetStatus(codes.Ok, "triage evaluation complete")

	return &contracts.TriageFlowOutput{
		IncidentID:     norm.IncidentID,
		FlowName:       norm.FlowName,
		LocalizedHop:   localizedCode,
		HopStatus:      status,
		PauseReason:    pReason,
		Severity:       sev,
		TriageAction:   action,
		RunbookURL:     runbook,
		CCTLActionLink: cctl,
		TriagedAt:      now.Format(time.RFC3339),
	}, nil
}
