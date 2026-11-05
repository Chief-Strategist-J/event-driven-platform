package tests

import (
	"context"
	"testing"

	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/domain/machines"
	"event-driven-platform/src/features/backpressure/domain/rules"
	"event-driven-platform/src/features/backpressure/infrastructure/adapters"
	"event-driven-platform/src/features/backpressure/service"
)

func buildService() *service.BackpressureService {
	return service.NewBackpressureService(adapters.NewBackpressureMemoryRepositoryAdapter())
}

func saturationInput(tenantID, handlerID, dependencyID string, pauseReason contracts.PauseReason, signal contracts.SaturationSignal) contracts.ReportSaturationInput {
	signal.HandlerID = handlerID
	signal.DependencyID = dependencyID
	return contracts.ReportSaturationInput{
		TenantID:      tenantID,
		SourceService: "test-service",
		ExternalID:    "ext-001",
		CorrelationID: "corr-001",
		Signal:        signal,
		PauseReason:   pauseReason,
	}
}

func recoveryInput(tenantID, handlerID, dependencyID string) contracts.ReportRecoveryInput {
	return contracts.ReportRecoveryInput{
		TenantID:      tenantID,
		SourceService: "test-service",
		ExternalID:    "ext-001",
		CorrelationID: "corr-001",
		HandlerID:     handlerID,
		DependencyID:  dependencyID,
	}
}

func TestRuleEvaluateSaturation_MemoryBudget(t *testing.T) {
	signal := contracts.SaturationSignal{
		LiveHeapBytes:    750,
		MemoryLimitBytes: 1000,
	}
	result := rules.EvaluateSaturation(signal)
	if !result.IsSaturated {
		t.Fatal("expected IsSaturated=true when heap>=70% limit")
	}
	if result.SaturationReason != contracts.PauseReasonMemoryBudget {
		t.Fatalf("expected PauseReasonMemoryBudget, got %s", result.SaturationReason)
	}
}

func TestRuleEvaluateSaturation_RejectionCount(t *testing.T) {
	signal := contracts.SaturationSignal{
		RejectionCount:   5,
		MemoryLimitBytes: 1000,
		LiveHeapBytes:    100,
	}
	result := rules.EvaluateSaturation(signal)
	if !result.IsSaturated {
		t.Fatal("expected IsSaturated=true when rejections>0")
	}
	if result.SaturationReason != contracts.PauseReasonRejections {
		t.Fatalf("expected PauseReasonRejections, got %s", result.SaturationReason)
	}
}

func TestRuleEvaluateSaturation_LatencyGradient(t *testing.T) {
	signal := contracts.SaturationSignal{
		LatencyP99Ms:      600,
		BaselineLatencyMs: 100,
		MemoryLimitBytes:  1000,
		LiveHeapBytes:     100,
	}
	result := rules.EvaluateSaturation(signal)
	if !result.IsSaturated {
		t.Fatal("expected IsSaturated=true when p99>2x baseline")
	}
	if result.SaturationReason != contracts.PauseReasonDBLatency {
		t.Fatalf("expected PauseReasonDBLatency, got %s", result.SaturationReason)
	}
}

func TestRuleEvaluateSaturation_SteadyState(t *testing.T) {
	signal := contracts.SaturationSignal{
		LatencyP99Ms:      80,
		BaselineLatencyMs: 100,
		MemoryLimitBytes:  1000,
		LiveHeapBytes:     100,
	}
	result := rules.EvaluateSaturation(signal)
	if result.IsSaturated {
		t.Fatal("expected IsSaturated=false for healthy signal")
	}
}

func TestRuleComputeDecreasedConcurrency_Floor(t *testing.T) {
	if rules.ComputeDecreasedConcurrency(1) != 1 {
		t.Fatal("floor must stay at 1")
	}
}

func TestRuleComputeDecreasedConcurrency_Halves(t *testing.T) {
	result := rules.ComputeDecreasedConcurrency(10)
	if result != 5 {
		t.Fatalf("expected 5, got %d", result)
	}
}

func TestRuleComputeRampConcurrency_Floor(t *testing.T) {
	result := rules.ComputeRampConcurrency(1)
	if result < 1 {
		t.Fatal("ramp must respect floor of 1")
	}
}

func TestRuleComputeRampConcurrency_25Percent(t *testing.T) {
	result := rules.ComputeRampConcurrency(20)
	if result != 5 {
		t.Fatalf("expected 5 (25%% of 20), got %d", result)
	}
}

func TestRuleCanCoalesce_StateOnly(t *testing.T) {
	if !rules.CanCoalesce(contracts.OrderingState) {
		t.Fatal("O-STATE must be coalescable")
	}
	if rules.CanCoalesce(contracts.OrderingStrict) {
		t.Fatal("O-STRICT must never coalesce")
	}
	if rules.CanCoalesce(contracts.OrderingNone) {
		t.Fatal("O-NONE must not use CanCoalesce; uses sampling")
	}
}

func TestRuleMustNeverShed_StrictOnly(t *testing.T) {
	if !rules.MustNeverShed(contracts.OrderingStrict) {
		t.Fatal("O-STRICT must never shed")
	}
	if rules.MustNeverShed(contracts.OrderingNone) {
		t.Fatal("O-NONE may shed")
	}
}

func TestMachineTransition_NormalToSaturated(t *testing.T) {
	next, err := machines.TransitionTo(contracts.StateNormal, contracts.EventSaturationDetected)
	if err != nil {
		t.Fatal(err)
	}
	if next != contracts.StateSaturated {
		t.Fatalf("expected SATURATED, got %s", next)
	}
}

func TestMachineTransition_SaturatedToRecovering(t *testing.T) {
	next, err := machines.TransitionTo(contracts.StateSaturated, contracts.EventRecoveryStarted)
	if err != nil {
		t.Fatal(err)
	}
	if next != contracts.StateRecovering {
		t.Fatalf("expected RECOVERING, got %s", next)
	}
}

func TestMachineTransition_RecoveringToNormal(t *testing.T) {
	next, err := machines.TransitionTo(contracts.StateRecovering, contracts.EventRecoveryConfirmed)
	if err != nil {
		t.Fatal(err)
	}
	if next != contracts.StateNormal {
		t.Fatalf("expected NORMAL, got %s", next)
	}
}

func TestMachineTransition_RecoveringResaturates(t *testing.T) {
	next, err := machines.TransitionTo(contracts.StateRecovering, contracts.EventSaturationDetected)
	if err != nil {
		t.Fatal(err)
	}
	if next != contracts.StateSaturated {
		t.Fatalf("expected SATURATED on re-saturation, got %s", next)
	}
}

func TestMachineTransition_InvalidRejected(t *testing.T) {
	_, err := machines.TransitionTo(contracts.StateNormal, contracts.EventRecoveryConfirmed)
	if err == nil {
		t.Fatal("expected error for invalid transition NORMAL->RECOVERY_CONFIRMED")
	}
}

func TestServiceReportSaturation_MemorySignal(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	signal := contracts.SaturationSignal{
		HandlerID:        "h1",
		DependencyID:     "db-primary",
		LiveHeapBytes:    750,
		MemoryLimitBytes: 1000,
	}
	input := saturationInput("tenant-1", "h1", "db-primary", contracts.PauseReasonMemoryBudget, signal)

	rec, err := svc.ReportSaturation(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.State != contracts.StateSaturated {
		t.Fatalf("expected SATURATED, got %s", rec.State)
	}
	if rec.PauseReason != contracts.PauseReasonMemoryBudget {
		t.Fatalf("expected PauseReasonMemoryBudget, got %s", rec.PauseReason)
	}
	if rec.PausedAt == nil {
		t.Fatal("PausedAt must be set when saturated")
	}
	if rec.ConcurrencyLimit < 1 {
		t.Fatal("ConcurrencyLimit must respect floor of 1")
	}
}

func TestServiceReportSaturation_MissingHandlerID(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	input := contracts.ReportSaturationInput{
		TenantID:      "tenant-1",
		SourceService: "svc",
		ExternalID:    "ext",
		CorrelationID: "corr",
		Signal:        contracts.SaturationSignal{HandlerID: "", DependencyID: "db"},
		PauseReason:   contracts.PauseReasonMemoryBudget,
	}
	_, err := svc.ReportSaturation(ctx, input)
	if err == nil {
		t.Fatal("expected validation error for missing handlerId")
	}
}

func TestServiceReportSaturation_NonSaturatedSignalRejected(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	signal := contracts.SaturationSignal{
		HandlerID:        "h1",
		DependencyID:     "db",
		LiveHeapBytes:    100,
		MemoryLimitBytes: 1000,
		LatencyP99Ms:     80,
		BaselineLatencyMs: 100,
	}
	input := saturationInput("tenant-1", "h1", "db", contracts.PauseReasonDBLatency, signal)
	_, err := svc.ReportSaturation(ctx, input)
	if err == nil {
		t.Fatal("expected error: signal does not indicate saturation")
	}
}

func TestServiceRecovery_FullLifecycle(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	signal := contracts.SaturationSignal{
		HandlerID:        "h2",
		DependencyID:     "db-replica",
		LiveHeapBytes:    750,
		MemoryLimitBytes: 1000,
	}
	satInput := saturationInput("tenant-2", "h2", "db-replica", contracts.PauseReasonMemoryBudget, signal)
	_, err := svc.ReportSaturation(ctx, satInput)
	if err != nil {
		t.Fatalf("saturation: %v", err)
	}

	recInput := recoveryInput("tenant-2", "h2", "db-replica")
	recRec, err := svc.ReportRecovery(ctx, recInput)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if recRec.State != contracts.StateRecovering {
		t.Fatalf("expected RECOVERING, got %s", recRec.State)
	}
	if recRec.ConcurrencyLimit < 1 {
		t.Fatal("ramp limit must be at least 1")
	}

	confirmed, err := svc.ConfirmRecovery(ctx, recInput)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if confirmed.State != contracts.StateNormal {
		t.Fatalf("expected NORMAL after confirm, got %s", confirmed.State)
	}
}

func TestServiceGetState_AfterSaturation(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	signal := contracts.SaturationSignal{
		HandlerID:        "h3",
		DependencyID:     "search",
		LiveHeapBytes:    800,
		MemoryLimitBytes: 1000,
	}
	satInput := saturationInput("tenant-3", "h3", "search", contracts.PauseReasonMemoryBudget, signal)
	_, err := svc.ReportSaturation(ctx, satInput)
	if err != nil {
		t.Fatalf("saturation: %v", err)
	}

	stateInput := contracts.GetBackpressureStateInput{
		TenantID:     "tenant-3",
		HandlerID:    "h3",
		DependencyID: "search",
	}
	result, err := svc.GetState(ctx, stateInput)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if result.State != contracts.StateSaturated {
		t.Fatalf("expected SATURATED, got %s", result.State)
	}
	if result.PausedAt == nil {
		t.Fatal("PausedAt must be set")
	}
	if result.PauseDurationMs < 0 {
		t.Fatal("PauseDurationMs must not be negative")
	}
}

func TestServiceListByTenant(t *testing.T) {
	svc := buildService()
	ctx := context.Background()

	for i, dep := range []string{"db", "search", "redis"} {
		signal := contracts.SaturationSignal{
			HandlerID:        "h1",
			DependencyID:     dep,
			LiveHeapBytes:    int64(700 + i*10),
			MemoryLimitBytes: 1000,
		}
		inp := saturationInput("tenant-list", "h1", dep, contracts.PauseReasonMemoryBudget, signal)
		_, err := svc.ReportSaturation(ctx, inp)
		if err != nil {
			t.Fatalf("saturation for dep %s: %v", dep, err)
		}
	}

	recs, err := svc.ListByTenant(ctx, "tenant-list")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
}

func TestSchemaValidation_MissingTenantID(t *testing.T) {
	_, err := contracts.ValidateReportSaturationInput(contracts.ReportSaturationInput{
		TenantID: "",
		Signal:   contracts.SaturationSignal{HandlerID: "h", DependencyID: "d"},
	})
	if err == nil {
		t.Fatal("expected error for missing tenantId")
	}
}

func TestSchemaValidation_MissingDependencyID(t *testing.T) {
	_, err := contracts.ValidateReportSaturationInput(contracts.ReportSaturationInput{
		TenantID:      "t",
		SourceService: "s",
		ExternalID:    "e",
		CorrelationID: "c",
		Signal:        contracts.SaturationSignal{HandlerID: "h", DependencyID: ""},
	})
	if err == nil {
		t.Fatal("expected error for missing dependencyId")
	}
}

func TestSchemaValidation_NormalizesWhitespace(t *testing.T) {
	normalized, err := contracts.ValidateReportSaturationInput(contracts.ReportSaturationInput{
		TenantID:      "  tenant-bp  ",
		SourceService: "  svc-bp  ",
		ExternalID:    " ext-bp ",
		CorrelationID: " corr-bp ",
		Signal: contracts.SaturationSignal{
			HandlerID:    " handler-bp ",
			DependencyID: " dep-bp ",
		},
	})
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if normalized.TenantID != "tenant-bp" {
		t.Fatalf("expected tenant-bp, got '%s'", normalized.TenantID)
	}
	if normalized.SourceService != "svc-bp" {
		t.Fatalf("expected svc-bp, got '%s'", normalized.SourceService)
	}
	if normalized.Signal.HandlerID != "handler-bp" {
		t.Fatalf("expected handler-bp, got '%s'", normalized.Signal.HandlerID)
	}
	if normalized.Signal.DependencyID != "dep-bp" {
		t.Fatalf("expected dep-bp, got '%s'", normalized.Signal.DependencyID)
	}
}

