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

	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/infrastructure/adapters"
	"event-driven-platform/src/features/backpressure/service"
	"event-driven-platform/src/shared/utils"
)

func getBackpressureTestDB(t *testing.T) *sql.DB {
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

// 1. PHYSICAL DATABASE CONSTRAINT INTEGRATION TESTS (09-test-writing.md Tier 3)
func TestBackpressure_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getBackpressureTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_bp_%d", time.Now().UnixNano())
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		invalidPublicID := "invalid_prefix_bp_123"

		query := `INSERT INTO backpressure_state (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			handler_id, dependency_id, state, pause_reason, concurrency_limit, paused_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			validUUID, invalidPublicID, tenantID, "order-worker", "ext-01", corrUUID,
			"order-handler", "orders-db", "SATURATED", "MEMORY_BUDGET", 5, now,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_backpressure_state_public_id_format") {
			t.Fatalf("expected ck_backpressure_state_public_id_format violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidStateEnum", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("bp")

		query := `INSERT INTO backpressure_state (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			handler_id, dependency_id, state, pause_reason, concurrency_limit, paused_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-worker", "ext-02", corrUUID,
			"order-handler", "orders-db", "UNKNOWN_STATE", "MEMORY_BUDGET", 5, now,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid state enum, got nil")
		}
		if !strings.Contains(err.Error(), "ck_backpressure_state_valid_state") {
			t.Fatalf("expected ck_backpressure_state_valid_state violation, got: %v", err)
		}
	})

	t.Run("RejectZeroOrNegativeConcurrencyLimit", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("bp")

		query := `INSERT INTO backpressure_state (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			handler_id, dependency_id, state, pause_reason, concurrency_limit, paused_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-worker", "ext-03", corrUUID,
			"order-handler", "orders-db", "SATURATED", "MEMORY_BUDGET", 0, now,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for concurrency_limit=0, got nil")
		}
		if !strings.Contains(err.Error(), "ck_backpressure_state_concurrency_min") {
			t.Fatalf("expected ck_backpressure_state_concurrency_min violation, got: %v", err)
		}
	})

	t.Run("RejectSaturatedWithoutPausedAt", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("bp")

		query := `INSERT INTO backpressure_state (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			handler_id, dependency_id, state, pause_reason, concurrency_limit, paused_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-worker", "ext-04", corrUUID,
			"order-handler", "orders-db", "SATURATED", "MEMORY_BUDGET", 5, nil,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for SATURATED without paused_at, got nil")
		}
		if !strings.Contains(err.Error(), "ck_backpressure_state_paused_at_saturated") {
			t.Fatalf("expected ck_backpressure_state_paused_at_saturated violation, got: %v", err)
		}
	})
}

// 2. REAL POSTGRESQL STATE LIFECYCLE & AUDIT LOG IMMUTABILITY
func TestBackpressure_Integration_LifecycleAndAuditImmutability(t *testing.T) {
	db := getBackpressureTestDB(t)
	defer db.Close()

	repo := adapters.NewBackpressurePostgresRepositoryAdapter(db)
	svc := service.NewBackpressureService(repo)

	tenantID := fmt.Sprintf("tenant_bp_life_%d", time.Now().UnixNano())
	handlerID := "payment-worker"
	dependencyID := "postgres-payment-db"
	ctx := context.Background()

	corrUUID, _ := utils.GenerateUUIDv7()

	// Step 1: Report Saturation
	satInput := contracts.ReportSaturationInput{
		TenantID:      tenantID,
		SourceService: "payment-service",
		ExternalID:    "ext-sat-01",
		CorrelationID: corrUUID,
		PauseReason:   contracts.PauseReasonMemoryBudget,
		Signal: contracts.SaturationSignal{
			HandlerID:        handlerID,
			DependencyID:     dependencyID,
			LiveHeapBytes:    850,
			MemoryLimitBytes: 1000,
		},
	}

	satRecord, err := svc.ReportSaturation(ctx, satInput)
	if err != nil {
		t.Fatalf("failed to report saturation against real postgres: %v", err)
	}
	if satRecord.State != contracts.StateSaturated {
		t.Fatalf("expected state SATURATED, got %s", satRecord.State)
	}
	if satRecord.ConcurrencyLimit <= 0 {
		t.Fatalf("expected concurrency_limit >= 1, got %d", satRecord.ConcurrencyLimit)
	}

	// Verify persistence directly in DB
	var dbState string
	var dbLimit int64
	err = db.QueryRowContext(ctx,
		"SELECT state, concurrency_limit FROM backpressure_state WHERE tenant_id = $1 AND handler_id = $2 AND dependency_id = $3",
		tenantID, handlerID, dependencyID,
	).Scan(&dbState, &dbLimit)
	if err != nil {
		t.Fatalf("failed to query backpressure_state in postgres: %v", err)
	}
	if dbState != "SATURATED" || dbLimit != satRecord.ConcurrencyLimit {
		t.Fatalf("mismatched postgres state: state=%s, limit=%d", dbState, dbLimit)
	}

	// Step 2: Report Recovery
	recInput := contracts.ReportRecoveryInput{
		TenantID:      tenantID,
		SourceService: "payment-service",
		ExternalID:    "ext-rec-01",
		CorrelationID: corrUUID,
		HandlerID:     handlerID,
		DependencyID:  dependencyID,
	}

	recRecord, err := svc.ReportRecovery(ctx, recInput)
	if err != nil {
		t.Fatalf("failed to report recovery against real postgres: %v", err)
	}
	if recRecord.State != contracts.StateRecovering {
		t.Fatalf("expected state RECOVERING, got %s", recRecord.State)
	}

	// Step 3: Confirm Recovery (Normal)
	confRecord, err := svc.ConfirmRecovery(ctx, recInput)
	if err != nil {
		t.Fatalf("failed to confirm recovery against real postgres: %v", err)
	}
	if confRecord.State != contracts.StateNormal {
		t.Fatalf("expected state NORMAL, got %s", confRecord.State)
	}

	// Step 4: Verify Audit Log count and trigger-enforced Immutability
	var auditCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM backpressure_audit_log WHERE tenant_id = $1 AND handler_id = $2",
		tenantID, handlerID,
	).Scan(&auditCount)
	if err != nil {
		t.Fatalf("failed to query audit log count: %v", err)
	}
	if auditCount != 3 {
		t.Fatalf("expected 3 audit log records (saturation, recovery, confirm), got %d", auditCount)
	}

	// Attempt mutation on backpressure_audit_log (Must be blocked by database trigger)
	_, mutErr := db.ExecContext(ctx,
		"UPDATE backpressure_audit_log SET reason = 'tampered' WHERE tenant_id = $1",
		tenantID,
	)
	if mutErr == nil {
		t.Fatalf("expected database immutability trigger violation on audit log update, got nil")
	}
	if !strings.Contains(mutErr.Error(), "append-only") && !strings.Contains(mutErr.Error(), "immutable") {
		t.Fatalf("expected immutable trigger error on audit log update, got: %v", mutErr)
	}
}

// 3. CONCURRENT WORKER STRESS TEST (09-test-writing.md Tier 4 Concurrency Invariant)
func TestBackpressure_Integration_ConcurrentSaturationContention(t *testing.T) {
	db := getBackpressureTestDB(t)
	defer db.Close()

	repo := adapters.NewBackpressurePostgresRepositoryAdapter(db)
	svc := service.NewBackpressureService(repo)

	tenantID := fmt.Sprintf("tenant_bp_conc_%d", time.Now().UnixNano())
	handlerID := "concurrent-handler"
	dependencyID := "shared-redis-cache"
	ctx := context.Background()

	const concurrency = 10
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			workerCorrUUID, _ := utils.GenerateUUIDv7()
			input := contracts.ReportSaturationInput{
				TenantID:      tenantID,
				SourceService: "stress-worker",
				ExternalID:    fmt.Sprintf("ext-conc-%d", workerID),
				CorrelationID: workerCorrUUID,
				PauseReason:   contracts.PauseReasonRejections,
				Signal: contracts.SaturationSignal{
					HandlerID:        handlerID,
					DependencyID:     dependencyID,
					RejectionCount:   10,
					MemoryLimitBytes: 1000,
					LiveHeapBytes:    100,
				},
			}
			_, err := svc.ReportSaturation(ctx, input)
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected concurrent worker saturation error: %v", err)
	}

	// Verify exactly one state record remains for the unique handler/dependency tuple
	var recordCount int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM backpressure_state WHERE tenant_id = $1 AND handler_id = $2 AND dependency_id = $3",
		tenantID, handlerID, dependencyID,
	).Scan(&recordCount)
	if err != nil {
		t.Fatalf("failed to query backpressure_state count: %v", err)
	}
	if recordCount != 1 {
		t.Fatalf("expected exactly 1 coalesced state row, got %d", recordCount)
	}
}

// 4. DISTRIBUTED TRACING & ERROR TELEMETRY COMPLIANCE (10-tracing-writing.md)
func TestBackpressure_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	repo := adapters.NewBackpressureMemoryRepositoryAdapter()
	svc := service.NewBackpressureService(repo)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.ReportSaturationInput{
			TenantID: "", // Missing mandatory tenant
		}

		_, err := svc.ReportSaturation(ctx, invalidInput)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		rootSpan := spans[0]
		if rootSpan.Name != "backpressure.report_saturation" {
			t.Fatalf("expected span name 'backpressure.report_saturation', got '%s'", rootSpan.Name)
		}

		// Verify 10-tracing-writing.md rule: Status must be ERROR on failure
		if rootSpan.Status.Code.String() != "Error" {
			t.Fatalf("expected span status Error, got %s", rootSpan.Status.Code.String())
		}

		// Verify error event is present on span
		if len(rootSpan.Events) == 0 {
			t.Fatalf("expected span to have recorded error event")
		}
		foundErrorEvent := false
		for _, ev := range rootSpan.Events {
			if ev.Name == "exception" {
				foundErrorEvent = true
				break
			}
		}
		if !foundErrorEvent {
			t.Fatalf("expected 'exception' event in span events: %+v", rootSpan.Events)
		}
	})

	t.Run("SpanAttributesRecordedOnSuccessfulSaturation", func(t *testing.T) {
		exporter.Reset()

		corrUUID, _ := utils.GenerateUUIDv7()
		validInput := contracts.ReportSaturationInput{
			TenantID:      "tenant-trace-test",
			SourceService: "trace-tester",
			ExternalID:    "ext-trace-01",
			CorrelationID: corrUUID,
			PauseReason:   contracts.PauseReasonMemoryBudget,
			Signal: contracts.SaturationSignal{
				HandlerID:        "trace-handler",
				DependencyID:     "trace-dep",
				LiveHeapBytes:    800,
				MemoryLimitBytes: 1000,
			},
		}

		res, err := svc.ReportSaturation(ctx, validInput)
		if err != nil {
			t.Fatalf("unexpected saturation failure: %v", err)
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span, got none")
		}

		var targetSpan *tracetest.SpanStub
		for i := range spans {
			if spans[i].Name == "backpressure.report_saturation" {
				targetSpan = &spans[i]
				break
			}
		}

		if targetSpan == nil {
			t.Fatalf("span 'backpressure.report_saturation' not found")
		}

		// Verify span attributes
		attrMap := make(map[string]any)
		for _, attr := range targetSpan.Attributes {
			attrMap[string(attr.Key)] = attr.Value.AsInterface()
		}

		if attrMap["tenant_id"] != "tenant-trace-test" {
			t.Fatalf("expected tenant_id attribute, got %v", attrMap["tenant_id"])
		}
		if attrMap["bp.public_id"] != res.PublicID {
			t.Fatalf("expected bp.public_id attribute %s, got %v", res.PublicID, attrMap["bp.public_id"])
		}
		if attrMap["bp.state"] != "SATURATED" {
			t.Fatalf("expected bp.state attribute SATURATED, got %v", attrMap["bp.state"])
		}
	})
}
