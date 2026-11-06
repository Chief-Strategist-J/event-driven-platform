package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-driven-platform/src/features/distributed-tracing/contracts"
	"event-driven-platform/src/features/distributed-tracing/infrastructure/adapters"
	"event-driven-platform/src/features/distributed-tracing/service"
)

func getTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
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
		t.Skipf("skipping live database test: %v", err)
	}
	return db
}

func TestDistributedTracing_PostgresLifecycle(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresDistributedTracingAdapter(db)
	svc := service.NewDistributedTracingService(repo)

	tenantID := "tenant_dt_test_" + uuid.New().String()[:8]
	svcName := "order_service_" + uuid.New().String()[:8]

	policy := contracts.TraceSamplingPolicy{
		TenantID:           tenantID,
		SourceService:      svcName,
		ExternalID:         "ext_pol_" + uuid.New().String()[:8],
		CorrelationID:      uuid.New(),
		ServiceName:        svcName,
		FlowName:           "checkout_flow",
		SampleRate:         0.5000,
		Priority:           50,
		AlwaysSampleErrors: true,
		MinDurationMs:      250,
	}

	regPolicy, err := svc.RegisterSamplingPolicy(ctx, policy)
	require.NoError(t, err)
	assert.NotEmpty(t, regPolicy.PublicID)

	evalRes, err := svc.EvaluateSampling(ctx, contracts.EvaluateSamplingCommand{
		TenantID:      tenantID,
		SourceService: svcName,
		FlowName:      "checkout_flow",
		CorrelationID: uuid.New().String(),
		DurationMs:    300,
		HasError:      false,
	})
	require.NoError(t, err)
	assert.True(t, evalRes.ShouldSample)
	assert.Equal(t, "LATENCY_THRESHOLD_OVERRIDE", evalRes.DecisionRule)

	covRes, err := svc.ReportCoverage(ctx, contracts.ReportCoverageCommand{
		TenantID:        tenantID,
		SourceService:   svcName,
		ExternalID:      "ext_cov_" + uuid.New().String()[:8],
		CorrelationID:   uuid.New().String(),
		HopSource:       svcName,
		HopTarget:       "billing_service",
		HopType:         contracts.HopTypeOutboxRelay,
		TotalSpans:      200,
		PropagatedSpans: 198,
	})
	require.NoError(t, err)
	assert.True(t, covRes.IsHealthy)
	assert.True(t, covRes.CoverageRatio >= 0.95)
}

