package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/adapters"
	"event-driven-platform/src/features/replay-mechanism/service"
	"event-driven-platform/src/shared/utils"
)

func getReplayTestDB(t *testing.T) *sql.DB {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "user=postgres password=postgres host=127.0.0.1 port=5432 dbname=event_store sslmode=disable"
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Skipf("skipping integration test, postgres connection error: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("skipping integration test, postgres unreachable: %v", err)
	}
	return db
}

func TestReplay_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getReplayTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_rpl_%d", time.Now().UnixNano())
	ctx := context.Background()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		invalidPublicID := "invalid_prefix_rpl_123"

		query := `INSERT INTO replay_run (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			target_projection, mode, state, scope, start_offset, current_offset, end_offset, replay_consumer_group
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			validUUID, invalidPublicID, tenantID, "order-service", "ext-01", corrUUID,
			"orders_view", "REBUILD", "INITIALIZING", "GLOBAL_TOPIC", 0, 0, 100, "replay.orders_view.01",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_replay_run_public_id_format") {
			t.Fatalf("expected ck_replay_run_public_id_format violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidModeEnum", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("rpl")

		query := `INSERT INTO replay_run (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			target_projection, mode, state, scope, start_offset, current_offset, end_offset, replay_consumer_group
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-02", corrUUID,
			"orders_view", "UNKNOWN_MODE", "INITIALIZING", "GLOBAL_TOPIC", 0, 0, 100, "replay.orders_view.02",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid mode enum, got nil")
		}
		if !strings.Contains(err.Error(), "ck_replay_run_mode") {
			t.Fatalf("expected ck_replay_run_mode violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidStateEnum", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("rpl")

		query := `INSERT INTO replay_run (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			target_projection, mode, state, scope, start_offset, current_offset, end_offset, replay_consumer_group
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-03", corrUUID,
			"orders_view", "REBUILD", "INVALID_STATE", "GLOBAL_TOPIC", 0, 0, 100, "replay.orders_view.03",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid state enum, got nil")
		}
		if !strings.Contains(err.Error(), "ck_replay_run_state") {
			t.Fatalf("expected ck_replay_run_state violation, got: %v", err)
		}
	})

	t.Run("RejectNegativeStartOffset", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("rpl")

		query := `INSERT INTO replay_run (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			target_projection, mode, state, scope, start_offset, current_offset, end_offset, replay_consumer_group
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-04", corrUUID,
			"orders_view", "REBUILD", "INITIALIZING", "GLOBAL_TOPIC", -1, -1, 100, "replay.orders_view.04",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for start_offset < 0, got nil")
		}
		if !strings.Contains(err.Error(), "ck_replay_run_start_offset_non_neg") {
			t.Fatalf("expected ck_replay_run_start_offset_non_neg violation, got: %v", err)
		}
	})
}

func TestReplay_Integration_LifecycleAndAuditImmutability(t *testing.T) {
	db := getReplayTestDB(t)
	defer db.Close()

	repo := adapters.NewReplayPostgresRepositoryAdapter(db)
	svc := service.NewReplayService(repo)

	tenantID := fmt.Sprintf("tenant_rpl_life_%d", time.Now().UnixNano())
	ctx := context.Background()

	corrUUID, _ := utils.GenerateUUIDv7()

	createInput := contracts.CreateReplayRunInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		ExternalID:       "ext-run-01",
		CorrelationID:    corrUUID,
		TargetProjection: "orders_customer_summary",
		Mode:             contracts.ModeRebuild,
		Scope:            contracts.ScopeGlobalTopic,
		StartOffset:      0,
		EndOffset:        100,
	}

	run, err := svc.CreateReplayRun(ctx, createInput)
	if err != nil {
		t.Fatalf("failed to create replay run in real postgres: %v", err)
	}
	if run.State != contracts.StateInitializing {
		t.Fatalf("expected state INITIALIZING, got %s", run.State)
	}
	if run.Version != 1 {
		t.Fatalf("expected initial version 1, got %d", run.Version)
	}

	streamRun, err := svc.StartStreaming(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatalf("failed to transition to STREAMING: %v", err)
	}
	if streamRun.State != contracts.StateStreaming {
		t.Fatalf("expected state STREAMING, got %s", streamRun.State)
	}
	if streamRun.Version != 2 {
		t.Fatalf("expected optimistic version 2, got %d", streamRun.Version)
	}

	advanceInput := contracts.AdvanceReplayInput{
		TenantID:      tenantID,
		RunID:         run.ID,
		CurrentOffset: 100,
	}
	caughtUpRun, err := svc.AdvanceReplay(ctx, advanceInput)
	if err != nil {
		t.Fatalf("failed to advance replay: %v", err)
	}
	if caughtUpRun.State != contracts.StateCaughtUp {
		t.Fatalf("expected state CAUGHT_UP, got %s", caughtUpRun.State)
	}
	if caughtUpRun.Version != 3 {
		t.Fatalf("expected optimistic version 3, got %d", caughtUpRun.Version)
	}

	promotedRun, err := svc.PromoteReplay(ctx, contracts.PromoteReplayInput{
		TenantID:      tenantID,
		SourceService: "order-service",
		ExternalID:    "ext-promote-01",
		CorrelationID: corrUUID,
		RunID:         run.ID,
		Reason:        "verified promotion readiness",
	})
	if err != nil {
		t.Fatalf("failed to promote replay: %v", err)
	}
	if promotedRun.State != contracts.StatePromoted {
		t.Fatalf("expected state PROMOTED, got %s", promotedRun.State)
	}

	completedRun, err := svc.CompleteCutover(ctx, contracts.CompleteCutoverInput{
		TenantID:      tenantID,
		SourceService: "order-service",
		ExternalID:    "ext-cutover-01",
		CorrelationID: corrUUID,
		RunID:         run.ID,
		Reason:        "cutover completed successfully",
	})
	if err != nil {
		t.Fatalf("failed to complete cutover: %v", err)
	}
	if completedRun.State != contracts.StateCompleted {
		t.Fatalf("expected state COMPLETED, got %s", completedRun.State)
	}

	evalInput := contracts.EvaluateEffectInput{
		TenantID:       tenantID,
		SourceService:  "order-service",
		ExternalID:     "ext-effect-01",
		CorrelationID:  corrUUID,
		RunID:          run.ID,
		EffectType:     contracts.EffectExternalPayment,
		IdempotencyKey: "pay_idem_001",
		PayloadDigest:  "sha256-abc123",
	}
	effectDecision, err := svc.EvaluateEffect(ctx, evalInput)
	if err != nil {
		t.Fatalf("failed to evaluate effect in real postgres: %v", err)
	}
	if effectDecision.Decision != contracts.DecisionSuppressed {
		t.Fatalf("expected SUPPRESSED decision in REBUILD mode, got %s", effectDecision.Decision)
	}

	_, mutErr := db.ExecContext(ctx,
		"UPDATE replay_audit_log SET reason = 'tampered' WHERE tenant_id = $1",
		tenantID,
	)
	if mutErr == nil {
		t.Fatalf("expected database immutability trigger error on replay_audit_log update, got nil")
	}
	if !strings.Contains(mutErr.Error(), "append-only") && !strings.Contains(mutErr.Error(), "prohibited") {
		t.Fatalf("expected trigger error on audit log update, got: %v", mutErr)
	}

	_, mutEffectErr := db.ExecContext(ctx,
		"UPDATE replay_effect_log SET decision = 'TAMPERED' WHERE tenant_id = $1",
		tenantID,
	)
	if mutEffectErr == nil {
		t.Fatalf("expected database immutability trigger error on replay_effect_log update, got nil")
	}
	if !strings.Contains(mutEffectErr.Error(), "append-only") && !strings.Contains(mutEffectErr.Error(), "prohibited") {
		t.Fatalf("expected trigger error on effect log update, got: %v", mutEffectErr)
	}
}

func TestReplay_Integration_ConcurrentOffsetProgression(t *testing.T) {
	db := getReplayTestDB(t)
	defer db.Close()

	repo := adapters.NewReplayPostgresRepositoryAdapter(db)
	svc := service.NewReplayService(repo)

	tenantID := fmt.Sprintf("tenant_rpl_conc_%d", time.Now().UnixNano())
	ctx := context.Background()
	corrUUID, _ := utils.GenerateUUIDv7()

	run, err := svc.CreateReplayRun(ctx, contracts.CreateReplayRunInput{
		TenantID:         tenantID,
		SourceService:    "stress-service",
		ExternalID:       "ext-conc-run",
		CorrelationID:    corrUUID,
		TargetProjection: "stress_projection",
		Mode:             contracts.ModeRebuild,
		Scope:            contracts.ScopeGlobalTopic,
		StartOffset:      0,
		EndOffset:        1000,
	})
	if err != nil {
		t.Fatalf("failed to create replay run: %v", err)
	}

	_, err = svc.StartStreaming(ctx, tenantID, run.ID)
	if err != nil {
		t.Fatalf("failed to start streaming: %v", err)
	}

	const concurrency = 5
	var wg sync.WaitGroup

	for i := 1; i <= concurrency; i++ {
		wg.Add(1)
		offset := int64(i * 10)
		go func(o int64) {
			defer wg.Done()
			_, _ = svc.AdvanceReplay(ctx, contracts.AdvanceReplayInput{
				TenantID:      tenantID,
				RunID:         run.ID,
				CurrentOffset: o,
			})
		}(offset)
	}

	wg.Wait()

	fetched, err := svc.GetReplayRun(ctx, contracts.GetReplayRunInput{
		TenantID: tenantID,
		RunID:    run.ID,
	})
	if err != nil {
		t.Fatalf("failed to fetch updated replay run: %v", err)
	}
	if fetched.CurrentOffset <= 0 {
		t.Fatalf("expected current_offset > 0 after concurrent progression, got %d", fetched.CurrentOffset)
	}
	if fetched.Version < 3 {
		t.Fatalf("expected version counter >= 3, got %d", fetched.Version)
	}
}

func TestReplay_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	repo := adapters.NewReplayMemoryRepositoryAdapter()
	svc := service.NewReplayService(repo)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.CreateReplayRunInput{
			TenantID: "",
		}

		_, err := svc.CreateReplayRun(ctx, invalidInput)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span, found none")
		}

		rootSpan := spans[0]
		if rootSpan.Name != "replay.create_run" {
			t.Fatalf("expected span name 'replay.create_run', got '%s'", rootSpan.Name)
		}

		if rootSpan.Status.Code.String() != "Error" {
			t.Fatalf("expected span status Error, got %s", rootSpan.Status.Code.String())
		}

		foundErrorEvent := false
		for _, ev := range rootSpan.Events {
			if ev.Name == "exception" {
				foundErrorEvent = true
				break
			}
		}
		if !foundErrorEvent {
			t.Fatalf("expected 'exception' event in span events")
		}
	})

	t.Run("SpanAttributesRecordedOnSuccessfulRunCreation", func(t *testing.T) {
		exporter.Reset()

		corrUUID, _ := utils.GenerateUUIDv7()
		validInput := contracts.CreateReplayRunInput{
			TenantID:         "tenant-replay-trace",
			SourceService:    "trace-service",
			ExternalID:       "ext-trace-01",
			CorrelationID:    corrUUID,
			TargetProjection: "orders_trace_view",
			Mode:             contracts.ModeRebuild,
			Scope:            contracts.ScopeGlobalTopic,
			StartOffset:      0,
			EndOffset:        500,
		}

		res, err := svc.CreateReplayRun(ctx, validInput)
		if err != nil {
			t.Fatalf("unexpected failure: %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span, got none")
		}

		var targetSpan *tracetest.SpanStub
		for i := range spans {
			if spans[i].Name == "replay.create_run" {
				targetSpan = &spans[i]
				break
			}
		}

		if targetSpan == nil {
			t.Fatalf("span 'replay.create_run' not found")
		}

		attrMap := make(map[string]any)
		for _, attr := range targetSpan.Attributes {
			attrMap[string(attr.Key)] = attr.Value.AsInterface()
		}

		if attrMap["tenant_id"] != "tenant-replay-trace" {
			t.Fatalf("expected tenant_id attribute, got %v", attrMap["tenant_id"])
		}
		if attrMap["replay.public_id"] != res.PublicID {
			t.Fatalf("expected replay.public_id %s, got %v", res.PublicID, attrMap["replay.public_id"])
		}
	})
}
