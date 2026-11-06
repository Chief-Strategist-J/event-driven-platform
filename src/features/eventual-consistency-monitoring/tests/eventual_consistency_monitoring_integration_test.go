package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/adapters"
	"event-driven-platform/src/features/eventual-consistency-monitoring/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func getECMTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping live database test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping live database test: postgres unreachable: %v", err)
	}

	return db
}

func TestECM_PhysicalCheckConstraints(t *testing.T) {
	db := getECMTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_ecm_ck_" + uuid.New().String()[:8]

	t.Run("RejectMalformedWatermarkPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO consistency_watermark_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, stream_name, partition_id, high_watermark_offset, consumer_offset,
			offset_lag, freshness_lag_ms, watermark_status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_id", tenantID, "ecm-poller", "ext_01", uuid.New(),
			"view_orders", "orders.v1", 0, 100, 100, 0, 10, "IN_SYNC",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consistency_watermark_ledger_public_id_format") {
			t.Fatalf("expected ck_consistency_watermark_ledger_public_id_format violation, got: %v", err)
		}
	})

	t.Run("RejectNegativeOffsets", func(t *testing.T) {
		id := uuid.New()
		publicID := "ecw_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO consistency_watermark_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, stream_name, partition_id, high_watermark_offset, consumer_offset,
			offset_lag, freshness_lag_ms, watermark_status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "ecm-poller", "ext_02", uuid.New(),
			"view_orders", "orders.v1", 0, -1, 100, 0, 10, "IN_SYNC",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for negative offset, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consistency_watermark_ledger_offsets_nonneg") {
			t.Fatalf("expected ck_consistency_watermark_ledger_offsets_nonneg violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidWatermarkStatusEnum", func(t *testing.T) {
		id := uuid.New()
		publicID := "ecw_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO consistency_watermark_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, stream_name, partition_id, high_watermark_offset, consumer_offset,
			offset_lag, freshness_lag_ms, watermark_status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "ecm-poller", "ext_03", uuid.New(),
			"view_orders", "orders.v1", 0, 100, 100, 0, 10, "INVALID_STATUS",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid status, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consistency_watermark_ledger_status") {
			t.Fatalf("expected ck_consistency_watermark_ledger_status violation, got: %v", err)
		}
	})
}

func TestECM_PostgresLifecycleAndStateTransitions(t *testing.T) {
	db := getECMTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repo := adapters.NewPostgresConsistencyRepositoryAdapter(db)
	svc := service.NewConsistencyService(repo)

	tenantID := "tenant_ecm_" + uuid.New().String()[:8]
	viewName := "orders_summary_" + uuid.New().String()[:8]

	wmIn := contracts.ObserveWatermarkInput{
		TenantID:            tenantID,
		SourceService:       "ecm-poller",
		ExternalID:          "ext_wm_1",
		CorrelationID:       uuid.New().String(),
		ViewName:            viewName,
		StreamName:          "orders.v1",
		PartitionID:         0,
		HighWatermarkOffset: 100,
		ConsumerOffset:      100,
		FreshnessLagMs:      10,
	}

	wm, err := svc.ObserveWatermark(ctx, wmIn)
	require.NoError(t, err)
	assert.NotEmpty(t, wm.PublicID)
	assert.Equal(t, "IN_SYNC", wm.WatermarkStatus)

	watermarks, err := svc.GetViewWatermarks(ctx, tenantID, viewName)
	require.NoError(t, err)
	assert.NotEmpty(t, watermarks)
	assert.Equal(t, wm.ID, watermarks[0].ID)
	assert.Equal(t, int64(100), watermarks[0].HighWatermarkOffset)

	driftIn := contracts.EvaluateRecordInput{
		TenantID:          tenantID,
		ViewName:          viewName,
		AggregateType:     "Order",
		AggregateID:       "ord_9901",
		SourceExists:      true,
		ViewExists:        true,
		SourceVersion:     5,
		ViewVersion:       4,
		SourcePayloadHash: "hash_v5",
		ViewPayloadHash:   "hash_v4",
	}

	res, err := svc.EvaluateAndRecordDrift(ctx, driftIn, "ecm-reconciler", uuid.New().String())
	require.NoError(t, err)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftVersionLag, res.DriftClass)

	incidents, err := svc.GetActiveIncidents(ctx, tenantID, viewName)
	require.NoError(t, err)
	assert.NotEmpty(t, incidents)
	assert.Equal(t, contracts.IncidentDetected, incidents[0].Status)

	healed, err := svc.AttemptHealing(ctx, incidents[0].ID, tenantID, 3)
	require.NoError(t, err)
	assert.True(t, healed)
}

func TestECM_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	db := getECMTestDB(t)
	defer db.Close()

	repo := adapters.NewPostgresConsistencyRepositoryAdapter(db)
	svc := service.NewConsistencyService(repo)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.ObserveWatermarkInput{
			TenantID: "",
		}

		_, err := svc.ObserveWatermark(ctx, invalidInput)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		var errSpan tracetest.SpanStub
		found := false
		for _, sp := range spans {
			if sp.Name == "ConsistencyService.ObserveWatermark" {
				errSpan = sp
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected span 'ConsistencyService.ObserveWatermark' in recorded spans: %+v", spans)
		}

		if errSpan.Status.Code.String() != "Error" {
			t.Fatalf("expected span status Error, got %s", errSpan.Status.Code.String())
		}
	})

	t.Run("SpanRecordedOnSuccessfulWatermark", func(t *testing.T) {
		exporter.Reset()

		tenantID := "tenant_ecm_trace_" + uuid.New().String()[:8]
		viewName := "trace_view_" + uuid.New().String()[:8]

		validInput := contracts.ObserveWatermarkInput{
			TenantID:            tenantID,
			SourceService:       "ecm-poller",
			ExternalID:          "ext_trace_wm",
			CorrelationID:       uuid.New().String(),
			ViewName:            viewName,
			StreamName:          "orders.v1",
			PartitionID:         0,
			HighWatermarkOffset: 200,
			ConsumerOffset:      200,
			FreshnessLagMs:      5,
		}

		wm, err := svc.ObserveWatermark(ctx, validInput)
		if err != nil {
			t.Fatalf("unexpected error observing watermark: %v", err)
		}
		if wm.PublicID == "" {
			t.Fatalf("expected generated public_id")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		found := false
		for _, sp := range spans {
			if sp.Name == "ConsistencyService.ObserveWatermark" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected span 'ConsistencyService.ObserveWatermark' in recorded spans: %+v", spans)
		}
	})
}
