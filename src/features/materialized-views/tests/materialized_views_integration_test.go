package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/infrastructure/adapters"
	"event-driven-platform/src/features/materialized-views/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewsIntegration_PostgresLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping live database test: %v", err)
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping live database test: postgres unreachable: %v", err)
		return
	}

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
