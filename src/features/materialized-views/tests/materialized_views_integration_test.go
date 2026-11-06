package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/infrastructure/adapters"
	"event-driven-platform/src/features/materialized-views/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func getMVTestDB(t *testing.T) *sql.DB {
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

func TestMaterializedViews_PhysicalCheckConstraints(t *testing.T) {
	db := getMVTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_mv_ck_" + uuid.New().String()[:8]

	t.Run("RejectMalformedRegistryPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO materialized_view_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, view_class, primary_store, active_table_name, freshness_slo_ms, consistency_mode
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, "invalid_id_format", tenantID, "order-service", "ext_01", uuid.New(),
			"customer_balances", "V_STATE", "POSTGRES", "customer_balances", 1000, "EVENTUAL",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_materialized_view_registry_public_id_format") {
			t.Fatalf("expected ck_materialized_view_registry_public_id_format violation, got: %v", err)
		}
	})

	t.Run("RejectNonPositiveFreshnessSLO", func(t *testing.T) {
		id := uuid.New()
		publicID := "mvr_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO materialized_view_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, view_class, primary_store, active_table_name, freshness_slo_ms, consistency_mode
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_02", uuid.New(),
			"customer_balances_zero", "V_STATE", "POSTGRES", "customer_balances_zero", 0, "EVENTUAL",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for freshness_slo_ms=0, got nil")
		}
		if !strings.Contains(err.Error(), "ck_materialized_view_registry_freshness_slo_positive") {
			t.Fatalf("expected ck_materialized_view_registry_freshness_slo_positive violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidViewClassEnum", func(t *testing.T) {
		id := uuid.New()
		publicID := "mvr_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO materialized_view_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			view_name, view_class, primary_store, active_table_name, freshness_slo_ms, consistency_mode
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_03", uuid.New(),
			"customer_balances_bad_enum", "INVALID_CLASS", "POSTGRES", "customer_balances_bad_enum", 1000, "EVENTUAL",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid view_class, got nil")
		}
		if !strings.Contains(err.Error(), "ck_materialized_view_registry_view_class") {
			t.Fatalf("expected ck_materialized_view_registry_view_class violation, got: %v", err)
		}
	})
}

func TestMaterializedViewsIntegration_PostgresLifecycle(t *testing.T) {
	db := getMVTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := adapters.NewPostgresViewRepositoryAdapter(db)
	svc := service.NewViewService(adapter)

	tenantID := "tenant_mv_" + uuid.New().String()[:8]
	viewName := "view_" + uuid.New().String()[:8]
	extID := "ext_" + uuid.New().String()[:8]

	regIn := contracts.RegisterViewInput{
		TenantID:        tenantID,
		SourceService:   "test-suite",
		ExternalID:      extID,
		CorrelationID:   uuid.New().String(),
		ViewName:        viewName,
		ViewClass:       contracts.ViewClassState,
		PrimaryStore:    contracts.StorageTargetPostgres,
		ActiveTableName: viewName,
		FreshnessSLOMs:  1500,
		ConsistencyMode: contracts.ConsistencyModeEventual,
	}

	reg, err := svc.RegisterView(ctx, regIn)
	require.NoError(t, err)
	assert.NotEmpty(t, reg.PublicID)
	assert.Equal(t, viewName, reg.ViewName)

	fetched, err := svc.GetViewByPublicID(ctx, reg.PublicID)
	require.NoError(t, err)
	assert.Equal(t, reg.ID, fetched.ID)
	assert.Equal(t, tenantID, fetched.TenantID)

	aggID := "agg_" + uuid.New().String()[:8]
	projIn := contracts.ApplyProjectionInput{
		TenantID:      tenantID,
		SourceService: "test-suite",
		ExternalID:    "proj_ext_1",
		CorrelationID: uuid.New().String(),
		ViewName:      viewName,
		AggregateType: "customer",
		AggregateID:   aggID,
		AggregateSeq:  1,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"status":"ACTIVE"}`,
	}

	eval, err := svc.ApplyProjection(ctx, projIn)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionApply, eval.Decision)
	assert.Equal(t, int64(1), eval.IncomingSeq)

	dupEval, err := svc.ApplyProjection(ctx, projIn)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionDuplicate, dupEval.Decision)

	gapIn := contracts.ApplyProjectionInput{
		TenantID:      tenantID,
		SourceService: "test-suite",
		ExternalID:    "proj_ext_3",
		CorrelationID: uuid.New().String(),
		ViewName:      viewName,
		AggregateType: "customer",
		AggregateID:   aggID,
		AggregateSeq:  3,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"status":"UPGRADED"}`,
	}

	gapEval, err := svc.ApplyProjection(ctx, gapIn)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionGapHold, gapEval.Decision)
	assert.True(t, gapEval.IsQuarantined)

	rebuildID := "reb_" + uuid.New().String()[:8]
	rebIn := contracts.InitiateRebuildInput{
		TenantID:          tenantID,
		SourceService:     "test-suite",
		ExternalID:        "reb_ext_1",
		CorrelationID:     uuid.New().String(),
		RebuildID:         rebuildID,
		ViewName:          viewName,
		SourceStore:       contracts.StorageTargetPostgres,
		TargetShadowTable: viewName + "_shadow",
		TargetLSN:         "0/16B3748",
	}

	rebuildLog, err := svc.InitiateRebuild(ctx, rebIn)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseInitiated, rebuildLog.Phase)

	advIn := contracts.AdvanceRebuildInput{
		TenantID:      tenantID,
		RebuildID:     rebuildID,
		ExpectedPhase: contracts.RebuildPhaseInitiated,
		TargetPhase:   contracts.RebuildPhaseHistoricalReplay,
		CorrelationID: uuid.New().String(),
	}

	advancedLog, err := svc.AdvanceRebuild(ctx, advIn)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseHistoricalReplay, advancedLog.Phase)
}

func TestMaterializedViews_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	db := getMVTestDB(t)
	defer db.Close()

	adapter := adapters.NewPostgresViewRepositoryAdapter(db)
	svc := service.NewViewService(adapter)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.RegisterViewInput{
			TenantID: "",
		}

		_, err := svc.RegisterView(ctx, invalidInput)
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
			if sp.Name == "ViewService.RegisterView" {
				errSpan = sp
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected span 'ViewService.RegisterView' in recorded spans: %+v", spans)
		}

		if errSpan.Status.Code.String() != "Error" {
			t.Fatalf("expected span status Error, got %s", errSpan.Status.Code.String())
		}
	})

	t.Run("SpanAttributesRecordedOnSuccessfulRegistration", func(t *testing.T) {
		exporter.Reset()

		tenantID := "tenant_mv_trace_" + uuid.New().String()[:8]
		viewName := "trace_view_" + uuid.New().String()[:8]

		validInput := contracts.RegisterViewInput{
			TenantID:        tenantID,
			SourceService:   "test-suite",
			ExternalID:      "ext_trace_01",
			CorrelationID:   uuid.New().String(),
			ViewName:        viewName,
			ViewClass:       contracts.ViewClassState,
			PrimaryStore:    contracts.StorageTargetPostgres,
			ActiveTableName: viewName,
			FreshnessSLOMs:  1000,
			ConsistencyMode: contracts.ConsistencyModeEventual,
		}

		reg, err := svc.RegisterView(ctx, validInput)
		if err != nil {
			t.Fatalf("unexpected error registering view: %v", err)
		}
		if reg.PublicID == "" {
			t.Fatalf("expected generated public_id")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		hasExpectedSpan := false
		for _, sp := range spans {
			if sp.Name == "ViewService.RegisterView" {
				hasExpectedSpan = true
				break
			}
		}
		if !hasExpectedSpan {
			t.Fatalf("expected span 'ViewService.RegisterView' in recorded spans: %+v", spans)
		}
	})
}
