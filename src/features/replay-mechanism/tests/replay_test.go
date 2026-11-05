package tests

import (
	"context"
	"testing"
	"time"

	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/features/replay-mechanism/domain/machines"
	"event-driven-platform/src/features/replay-mechanism/domain/rules"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/adapters"
	"event-driven-platform/src/features/replay-mechanism/service"
)

func buildReplayService() *service.ReplayService {
	return service.NewReplayService(adapters.NewReplayMemoryRepositoryAdapter())
}

func sampleCreateInput(tenantID, proj string, mode contracts.ReplayRunMode, start, end int64) contracts.CreateReplayRunInput {
	return contracts.CreateReplayRunInput{
		TenantID:         tenantID,
		SourceService:    "test-service",
		ExternalID:       "ext-replay-001",
		CorrelationID:    "corr-replay-001",
		TargetProjection: proj,
		Mode:             mode,
		Scope:            contracts.ScopeGlobalTopic,
		StartOffset:      start,
		EndOffset:        end,
	}
}

func TestRuleGenerateReplayConsumerGroup(t *testing.T) {
	now := time.Unix(1700000000, 0)
	grp := rules.GenerateReplayConsumerGroup("orders_view", now)
	expected := "replay.orders_view.1700000000"
	if grp != expected {
		t.Fatalf("expected %s, got %s", expected, grp)
	}
}

func TestRuleResolveEffectMode_Rebuild(t *testing.T) {
	mode, err := rules.ResolveEffectMode(contracts.ModeRebuild, contracts.EffectExternalPayment)
	if err != nil || mode != contracts.EffectModeSuppress {
		t.Fatalf("expected SUPPRESS and nil err, got %s, %v", mode, err)
	}
	notificationMode, err := rules.ResolveEffectMode(contracts.ModeRebuild, contracts.EffectExternalNotification)
	if err != nil || notificationMode != contracts.EffectModeSuppress {
		t.Fatalf("expected SUPPRESS for notifications, got %s, %v", notificationMode, err)
	}
}

func TestRuleResolveEffectMode_Recover(t *testing.T) {
	mode, err := rules.ResolveEffectMode(contracts.ModeRecover, contracts.EffectExternalPayment)
	if err != nil || mode != contracts.EffectModeReconcile {
		t.Fatalf("expected RECONCILE and nil err, got %s, %v", mode, err)
	}
}

func TestRuleResolveEffectMode_Live(t *testing.T) {
	mode, err := rules.ResolveEffectMode(contracts.ModeLive, contracts.EffectExternalPayment)
	if err != nil || mode != contracts.EffectModeAllow {
		t.Fatalf("expected ALLOW and nil err, got %s, %v", mode, err)
	}
}

func TestRuleResolveEffectMode_SearchAndCacheAllowed(t *testing.T) {
	searchMode, err := rules.ResolveEffectMode(contracts.ModeRebuild, contracts.EffectSearchIndex)
	if err != nil || searchMode != contracts.EffectModeAllow {
		t.Fatalf("expected ALLOW for search index, got %s, %v", searchMode, err)
	}
	cacheMode, err := rules.ResolveEffectMode(contracts.ModeRebuild, contracts.EffectCacheWrite)
	if err != nil || cacheMode != contracts.EffectModeAllow {
		t.Fatalf("expected ALLOW for cache write, got %s, %v", cacheMode, err)
	}
}

func TestRuleResolveEffectMode_UnrecognizedRejects(t *testing.T) {
	_, err := rules.ResolveEffectMode("UNKNOWN_MODE", contracts.EffectExternalPayment)
	if err != rules.ErrUnrecognizedReplayMode {
		t.Fatalf("expected ErrUnrecognizedReplayMode, got %v", err)
	}
	_, err = rules.ResolveEffectMode(contracts.ModeRebuild, "UNKNOWN_EFFECT")
	if err != rules.ErrUnrecognizedEffectType {
		t.Fatalf("expected ErrUnrecognizedEffectType, got %v", err)
	}
}

func TestRuleValidateOffsetAdvance_Valid(t *testing.T) {
	err := rules.ValidateOffsetAdvance(100, 150, 200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuleValidateOffsetAdvance_RegressionRejected(t *testing.T) {
	err := rules.ValidateOffsetAdvance(100, 99, 200)
	if err != rules.ErrReplayOffsetRegression {
		t.Fatalf("expected ErrReplayOffsetRegression, got %v", err)
	}
}

func TestRuleValidateOffsetAdvance_ExceedsEndOffset(t *testing.T) {
	err := rules.ValidateOffsetAdvance(100, 250, 200)
	if err != rules.ErrReplayExceedsEndOffset {
		t.Fatalf("expected ErrReplayExceedsEndOffset, got %v", err)
	}
}

func TestRuleIsCaughtUp(t *testing.T) {
	if !rules.IsCaughtUp(200, 200) {
		t.Fatal("expected IsCaughtUp=true when current==end")
	}
	if !rules.IsCaughtUp(250, 200) {
		t.Fatal("expected IsCaughtUp=true when current>end")
	}
	if rules.IsCaughtUp(150, 200) {
		t.Fatal("expected IsCaughtUp=false when current<end")
	}
	if !rules.IsCaughtUp(150, 0) {
		t.Fatal("expected IsCaughtUp=true when end==0 (unbounded)")
	}
}

func TestMachineTransition_Lifecycle(t *testing.T) {
	state, err := machines.TransitionTo(contracts.StateInitializing, contracts.EventStartStreaming)
	if err != nil || state != contracts.StateStreaming {
		t.Fatalf("expected STREAMING, got %s (err: %v)", state, err)
	}
	state, err = machines.TransitionTo(state, contracts.EventFollowLive)
	if err != nil || state != contracts.StateFollowing {
		t.Fatalf("expected FOLLOWING, got %s (err: %v)", state, err)
	}
	state, err = machines.TransitionTo(state, contracts.EventCatchUpReached)
	if err != nil || state != contracts.StateCaughtUp {
		t.Fatalf("expected CAUGHT_UP, got %s (err: %v)", state, err)
	}
	state, err = machines.TransitionTo(state, contracts.EventPromoteWriter)
	if err != nil || state != contracts.StatePromoted {
		t.Fatalf("expected PROMOTED, got %s (err: %v)", state, err)
	}
	state, err = machines.TransitionTo(state, contracts.EventCompleteCutover)
	if err != nil || state != contracts.StateCompleted {
		t.Fatalf("expected COMPLETED, got %s (err: %v)", state, err)
	}
	if !machines.IsTerminal(state) {
		t.Fatal("COMPLETED must be terminal")
	}
}

func TestMachineTransition_AbortFromAnyActiveState(t *testing.T) {
	for _, s := range []contracts.ReplayState{
		contracts.StateInitializing,
		contracts.StateStreaming,
		contracts.StateFollowing,
		contracts.StateCaughtUp,
		contracts.StatePromoted,
	} {
		next, err := machines.TransitionTo(s, contracts.EventAbortRun)
		if err != nil || next != contracts.StateAborted {
			t.Fatalf("expected ABORTED from %s, got %s (err: %v)", s, next, err)
		}
	}
}

func TestMachineTransition_InvalidRejected(t *testing.T) {
	_, err := machines.TransitionTo(contracts.StateInitializing, contracts.EventCompleteCutover)
	if err != machines.ErrInvalidReplayTransition {
		t.Fatalf("expected ErrInvalidReplayTransition, got %v", err)
	}
}

func TestServiceCreateReplayRun(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-alpha", "orders_summary", contracts.ModeRebuild, 0, 1000)
	run, err := svc.CreateReplayRun(ctx, input)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if run.State != contracts.StateInitializing {
		t.Fatalf("expected INITIALIZING, got %s", run.State)
	}
	if run.Version != 1 {
		t.Fatalf("expected version 1, got %d", run.Version)
	}
	if run.ReplayConsumerGroup == "" {
		t.Fatal("replay consumer group must not be empty")
	}
}

func TestServiceStartStreaming(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-alpha", "orders_summary", contracts.ModeRebuild, 0, 1000)
	run, _ := svc.CreateReplayRun(ctx, input)

	started, err := svc.StartStreaming(ctx, "tenant-alpha", run.ID)
	if err != nil {
		t.Fatalf("start streaming: %v", err)
	}
	if started.State != contracts.StateStreaming {
		t.Fatalf("expected STREAMING, got %s", started.State)
	}
}

func TestServiceAdvanceReplay_CatchesUp(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-beta", "user_profiles", contracts.ModeHydrate, 100, 500)
	run, _ := svc.CreateReplayRun(ctx, input)
	_, _ = svc.StartStreaming(ctx, "tenant-beta", run.ID)

	advanced, err := svc.AdvanceReplay(ctx, contracts.AdvanceReplayInput{
		TenantID:      "tenant-beta",
		SourceService: "test-service",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		CurrentOffset: 300,
	})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if advanced.CurrentOffset != 300 {
		t.Fatalf("expected offset 300, got %d", advanced.CurrentOffset)
	}
	if advanced.State != contracts.StateStreaming {
		t.Fatalf("expected still STREAMING, got %s", advanced.State)
	}

	caughtUp, err := svc.AdvanceReplay(ctx, contracts.AdvanceReplayInput{
		TenantID:      "tenant-beta",
		SourceService: "test-service",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		CurrentOffset: 500,
	})
	if err != nil {
		t.Fatalf("advance to caught up: %v", err)
	}
	if caughtUp.State != contracts.StateCaughtUp {
		t.Fatalf("expected CAUGHT_UP, got %s", caughtUp.State)
	}
}

func TestServicePromoteAndCutover(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-gamma", "invoices_mv", contracts.ModeRebuild, 0, 200)
	run, _ := svc.CreateReplayRun(ctx, input)
	_, _ = svc.StartStreaming(ctx, "tenant-gamma", run.ID)
	_, _ = svc.AdvanceReplay(ctx, contracts.AdvanceReplayInput{
		TenantID:      "tenant-gamma",
		SourceService: "svc",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		CurrentOffset: 200,
	})

	promoted, err := svc.PromoteReplay(ctx, contracts.PromoteReplayInput{
		TenantID:      "tenant-gamma",
		SourceService: "svc",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		Reason:        "ready-for-dual-run",
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promoted.State != contracts.StatePromoted {
		t.Fatalf("expected PROMOTED, got %s", promoted.State)
	}

	completed, err := svc.CompleteCutover(ctx, contracts.CompleteCutoverInput{
		TenantID:      "tenant-gamma",
		SourceService: "svc",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		Reason:        "observation-window-passed",
	})
	if err != nil {
		t.Fatalf("complete cutover: %v", err)
	}
	if completed.State != contracts.StateCompleted {
		t.Fatalf("expected COMPLETED, got %s", completed.State)
	}
}

func TestServiceAbortReplay(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-delta", "failed_view", contracts.ModeRebuild, 0, 100)
	run, _ := svc.CreateReplayRun(ctx, input)

	aborted, err := svc.AbortReplay(ctx, contracts.AbortReplayInput{
		TenantID:      "tenant-delta",
		SourceService: "svc",
		ExternalID:    "ext",
		CorrelationID: "corr",
		RunID:         run.ID,
		Reason:        "projection-bug-detected",
	})
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if aborted.State != contracts.StateAborted {
		t.Fatalf("expected ABORTED, got %s", aborted.State)
	}
}

func TestServiceEvaluateEffect_SuppressExternalInRebuild(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-eff", "ledger_view", contracts.ModeRebuild, 0, 500)
	run, _ := svc.CreateReplayRun(ctx, input)

	eval, err := svc.EvaluateEffect(ctx, contracts.EvaluateEffectInput{
		TenantID:       "tenant-eff",
		SourceService:  "svc",
		ExternalID:     "ext-eff-1",
		CorrelationID:  "corr-1",
		RunID:          run.ID,
		EffectType:     contracts.EffectExternalPayment,
		IdempotencyKey: "pay-12345",
		PayloadDigest:  "digest-abc",
	})
	if err != nil {
		t.Fatalf("evaluate effect: %v", err)
	}
	if eval.Decision != contracts.DecisionSuppressed {
		t.Fatalf("expected SUPPRESSED, got %s", eval.Decision)
	}
	if eval.EffectMode != contracts.EffectModeSuppress {
		t.Fatalf("expected mode SUPPRESS, got %s", eval.EffectMode)
	}
}

func TestServiceEvaluateEffect_ReconcileInRecover(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	input := sampleCreateInput("tenant-eff", "ledger_view", contracts.ModeRecover, 0, 500)
	run, _ := svc.CreateReplayRun(ctx, input)

	eval, err := svc.EvaluateEffect(ctx, contracts.EvaluateEffectInput{
		TenantID:       "tenant-eff",
		SourceService:  "svc",
		ExternalID:     "ext-eff-2",
		CorrelationID:  "corr-2",
		RunID:          run.ID,
		EffectType:     contracts.EffectExternalPayment,
		IdempotencyKey: "pay-99999",
		PayloadDigest:  "digest-xyz",
	})
	if err != nil {
		t.Fatalf("evaluate effect: %v", err)
	}
	if eval.Decision != contracts.DecisionReconciled {
		t.Fatalf("expected RECONCILED, got %s", eval.Decision)
	}
}

func TestServiceListRunsByTenant(t *testing.T) {
	svc := buildReplayService()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		inp := sampleCreateInput("tenant-runs", "view", contracts.ModeRebuild, int64(i*10), int64(i*10+50))
		_, err := svc.CreateReplayRun(ctx, inp)
		if err != nil {
			t.Fatalf("create run: %v", err)
		}
	}

	runs, err := svc.ListReplayRuns(ctx, "tenant-runs")
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
}

func TestSchemaValidation_RejectsMissingFields(t *testing.T) {
	_, err := contracts.ValidateCreateReplayRunInput(contracts.CreateReplayRunInput{
		TenantID: "",
	})
	if err != contracts.ErrMissingTenantID {
		t.Fatalf("expected ErrMissingTenantID, got %v", err)
	}
	_, err = contracts.ValidateCreateReplayRunInput(contracts.CreateReplayRunInput{
		TenantID:         "t",
		SourceService:    "s",
		ExternalID:       "e",
		CorrelationID:    "c",
		TargetProjection: "",
	})
	if err != contracts.ErrMissingTargetProjection {
		t.Fatalf("expected ErrMissingTargetProjection, got %v", err)
	}
}

func TestSchemaValidation_RejectsNegativeOffsets(t *testing.T) {
	_, err := contracts.ValidateCreateReplayRunInput(contracts.CreateReplayRunInput{
		TenantID:         "t",
		SourceService:    "s",
		ExternalID:       "e",
		CorrelationID:    "c",
		TargetProjection: "proj",
		Mode:             contracts.ModeRebuild,
		Scope:            contracts.ScopeGlobalTopic,
		StartOffset:      -5,
		EndOffset:        100,
	})
	if err != contracts.ErrNegativeOffsetRange {
		t.Fatalf("expected ErrNegativeOffsetRange, got %v", err)
	}
}

func TestSchemaValidation_NormalizesWhitespaceKeys(t *testing.T) {
	normalized, err := contracts.ValidateCreateReplayRunInput(contracts.CreateReplayRunInput{
		TenantID:         "  tenant-space  ",
		SourceService:    "  service-space ",
		ExternalID:       " ext-1 ",
		CorrelationID:    " corr-1 ",
		TargetProjection: " proj-1 ",
		Mode:             contracts.ModeRebuild,
		Scope:            contracts.ScopeGlobalTopic,
		StartOffset:      0,
		EndOffset:        100,
	})
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if normalized.TenantID != "tenant-space" {
		t.Fatalf("expected trimmed tenant-space, got '%s'", normalized.TenantID)
	}
	if normalized.SourceService != "service-space" {
		t.Fatalf("expected trimmed service-space, got '%s'", normalized.SourceService)
	}
	if normalized.ExternalID != "ext-1" {
		t.Fatalf("expected trimmed ext-1, got '%s'", normalized.ExternalID)
	}
}

