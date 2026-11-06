package tests

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"event-driven-platform/src/features/distributed-tracing/contracts"
	"event-driven-platform/src/features/distributed-tracing/domain/machines"
	"event-driven-platform/src/features/distributed-tracing/domain/rules"
	"event-driven-platform/src/features/distributed-tracing/service"
)

func TestParseW3CTraceparent(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx, err := rules.ParseW3CTraceparent(valid)
	if err != nil {
		t.Fatalf("expected valid parse: %v", err)
	}
	if ctx.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected trace id: %s", ctx.TraceID)
	}

	invalidZeroTrace := "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
	if _, err := rules.ParseW3CTraceparent(invalidZeroTrace); err == nil {
		t.Fatalf("expected error for all-zero trace id")
	}

	invalidFormat := "4bf92f3577b34da6a3ce929d0e0e4736"
	if _, err := rules.ParseW3CTraceparent(invalidFormat); err == nil {
		t.Fatalf("expected error for invalid format")
	}
}

func TestSanitizeAttributes(t *testing.T) {
	raw := map[string]string{
		"auth":       "Bearer eyJhbGciOi...",
		"card":       "4111-2222-3333-4444",
		"normal_tag": "order_checkout",
	}
	sanitized := rules.SanitizeAttributes(raw)
	if !rules.EvaluateDeterministicHash("corr_1", 1.0) {
		t.Fatalf("expected 1.0 to sample")
	}
	if rules.EvaluateDeterministicHash("corr_1", 0.0) {
		t.Fatalf("expected 0.0 not to sample")
	}
	if len(sanitized) == 0 {
		t.Fatalf("expected sanitized json string")
	}
}

func TestClassifyLinkingMode(t *testing.T) {
	mode, link, child := rules.ClassifyLinkingMode(false, 1000)
	if !child || link || mode != "CHILD_SPAN_DIRECT" {
		t.Fatalf("expected direct child span for sub-minute message")
	}

	mode, link, child = rules.ClassifyLinkingMode(true, 1000)
	if child || !link || mode != "SPAN_LINK_BATCH" {
		t.Fatalf("expected span link for batch message")
	}

	mode, link, child = rules.ClassifyLinkingMode(false, 400000)
	if child || !link || mode != "SPAN_LINK_DELAYED" {
		t.Fatalf("expected span link for delayed backlog message")
	}
}

func TestSamplingStateMachine(t *testing.T) {
	sm := machines.NewSamplingStateMachine()
	policy := contracts.TraceSamplingPolicy{
		SampleRate:         0.0,
		AlwaysSampleErrors: true,
		MinDurationMs:      500,
	}

	errRes := sm.Evaluate(policy, contracts.EvaluateSamplingCommand{
		HasError:      true,
		CorrelationID: "corr_123",
	})
	if !errRes.ShouldSample || errRes.DecisionRule != "ERROR_TAIL_SAMPLING_OVERRIDE" {
		t.Fatalf("expected error override sampling")
	}

	slowRes := sm.Evaluate(policy, contracts.EvaluateSamplingCommand{
		DurationMs:    600,
		CorrelationID: "corr_123",
	})
	if !slowRes.ShouldSample || slowRes.DecisionRule != "LATENCY_THRESHOLD_OVERRIDE" {
		t.Fatalf("expected latency override sampling")
	}
}

type memoryTracingRepo struct {
	policies map[string]contracts.TraceSamplingPolicy
	audits   map[string]contracts.TraceSpanAudit
	coverage map[string]contracts.TracePropagationCoverage
}

func newMemoryTracingRepo() *memoryTracingRepo {
	return &memoryTracingRepo{
		policies: make(map[string]contracts.TraceSamplingPolicy),
		audits:   make(map[string]contracts.TraceSpanAudit),
		coverage: make(map[string]contracts.TracePropagationCoverage),
	}
}

func (m *memoryTracingRepo) SaveSamplingPolicy(_ context.Context, policy contracts.TraceSamplingPolicy) error {
	m.policies[policy.ServiceName] = policy
	return nil
}

func (m *memoryTracingRepo) GetSamplingPolicy(_ context.Context, _ string, serviceName string) (contracts.TraceSamplingPolicy, error) {
	if p, ok := m.policies[serviceName]; ok {
		return p, nil
	}
	return contracts.TraceSamplingPolicy{
		ServiceName:        serviceName,
		SampleRate:         1.0,
		AlwaysSampleErrors: true,
		IsActive:           true,
	}, nil
}

func (m *memoryTracingRepo) RecordSpanAudit(_ context.Context, audit contracts.TraceSpanAudit) error {
	m.audits[audit.SpanID] = audit
	return nil
}

func (m *memoryTracingRepo) UpsertPropagationCoverage(_ context.Context, cov contracts.TracePropagationCoverage) error {
	m.coverage[cov.HopSource] = cov
	return nil
}

func TestTracingServiceLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryTracingRepo()
	svc := service.NewDistributedTracingService(repo)

	injectRes, err := svc.InjectTraceContext(ctx, contracts.InjectTraceContextCommand{
		TenantID:      "tenant_1",
		SourceService: "order-service",
		CorrelationID: uuid.New().String(),
		Carrier:       make(map[string]string),
	})
	if err != nil {
		t.Fatalf("failed injecting trace context: %v", err)
	}
	if injectRes.Traceparent == "" {
		t.Fatalf("expected non-empty traceparent")
	}

	extractRes, err := svc.ExtractTraceContext(ctx, contracts.ExtractTraceContextCommand{
		Carrier:         injectRes.Carrier,
		IsBatch:         false,
		QueueWaitMillis: 500,
	})
	if err != nil {
		t.Fatalf("failed extracting trace context: %v", err)
	}
	if !extractRes.AttachAsChild {
		t.Fatalf("expected child attachment")
	}

	covRes, err := svc.ReportCoverage(ctx, contracts.ReportCoverageCommand{
		TenantID:        "tenant_1",
		SourceService:   "order-service",
		ExternalID:      "ext_cov_1",
		CorrelationID:   uuid.New().String(),
		HopSource:       "order-service",
		HopTarget:       "payment-service",
		HopType:         contracts.HopTypeOutboxRelay,
		TotalSpans:      100,
		PropagatedSpans: 99,
	})
	if err != nil {
		t.Fatalf("failed reporting coverage: %v", err)
	}
	if !covRes.IsHealthy || covRes.CoverageRatio < 0.95 {
		t.Fatalf("expected healthy coverage ratio")
	}
}
