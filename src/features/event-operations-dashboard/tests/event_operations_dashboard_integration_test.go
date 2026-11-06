package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
	"event-driven-platform/src/features/event-operations-dashboard/infrastructure/adapters"
	"event-driven-platform/src/features/event-operations-dashboard/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func getOpsTestDB(t *testing.T) *sql.DB {
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

func TestEventOpsDashboard_PostgresLifecycleAndTriage(t *testing.T) {
	db := getOpsTestDB(t)
	defer db.Close()
	ctx := context.Background()

	repo := adapters.NewPostgresEventOperationsDashboardRepository(db)
	svc := service.NewEventOperationsDashboardService(repo)

	tenantID := fmt.Sprintf("tenant_ops_%d", time.Now().UnixNano())
	flowName := "payment_processing"

	_, err := svc.ReportHopTelemetry(ctx, &contracts.ReportHopTelemetryInput{
		TenantID:      tenantID,
		SourceService: "outbox-relay",
		ExternalID:    "h1_rep",
		CorrelationID: uuid.New(),
		FlowName:      flowName,
		HopCode:       contracts.HopH1Outbox,
		Tier:          contracts.Tier0,
		Status:        contracts.StatusOk,
		LagSeconds:    0.5,
		RunbookURL:    "https://wiki/ops/outbox",
	})
	require.NoError(t, err)

	h4Rec, err := svc.ReportHopTelemetry(ctx, &contracts.ReportHopTelemetryInput{
		TenantID:      tenantID,
		SourceService: "payment-worker",
		ExternalID:    "h4_rep",
		CorrelationID: uuid.New(),
		FlowName:      flowName,
		HopCode:       contracts.HopH4Consumer,
		Tier:          contracts.Tier0,
		Status:        contracts.StatusPaused,
		PauseReason:   contracts.PauseReasonBackpressure,
		LagSeconds:    65.0,
		RunbookURL:    "https://wiki/ops/backpressure",
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidateHopPublicID(h4Rec.PublicID))

	triageOut, err := svc.TriageFlow(ctx, &contracts.TriageFlowInput{
		TenantID:      tenantID,
		SourceService: "ops-dashboard",
		ExternalID:    "triage_ext_01",
		CorrelationID: uuid.New(),
		IncidentID:    "inc_payment_99",
		FlowName:      flowName,
		TriagedBy:     "sre_lead@company.org",
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.HopH4Consumer, triageOut.LocalizedHop)
	assert.Equal(t, contracts.StatusPaused, triageOut.HopStatus)
	assert.Equal(t, contracts.Sev0, triageOut.Severity)
	assert.Contains(t, triageOut.TriageAction, "Backpressure active")
	assert.Contains(t, triageOut.CCTLActionLink, "/control-plane/consumers/override")
}

func TestEventOpsDashboard_PhysicalCheckConstraints(t *testing.T) {
	db := getOpsTestDB(t)
	defer db.Close()
	ctx := context.Background()

	t.Run("RejectMalformedHopPublicID", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO ops_pipeline_hop_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				flow_name, hop_code, tier, status, pause_reason, lag_seconds,
				oldest_held_age_seconds, runbook_url
			) VALUES (
				gen_random_uuid(), 'bad_hop', 't1', 'src', 'ext1', gen_random_uuid(),
				'fl1', 'H1_OUTBOX', 'T0', 'OK', 'NONE', 0, 0, 'https://runbook'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_ops_pipeline_hop_public_id_format")
	})

	t.Run("RejectInvalidHopCode", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO ops_pipeline_hop_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				flow_name, hop_code, tier, status, pause_reason, lag_seconds,
				oldest_held_age_seconds, runbook_url
			) VALUES (
				gen_random_uuid(), 'hop_1234567890abcdef', 't1', 'src', 'ext2', gen_random_uuid(),
				'fl1', 'INVALID_HOP', 'T0', 'OK', 'NONE', 0, 0, 'https://runbook'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_ops_pipeline_hop_code")
	})

	t.Run("RejectTriageLogMutationViaTrigger", func(t *testing.T) {
		pubID, _ := contracts.GeneratePublicID("itl")
		_, err := db.ExecContext(ctx, `
			INSERT INTO ops_incident_triage_log (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				incident_id, flow_name, localized_hop, severity, triage_action,
				c_ctl_action_link, triaged_by
			) VALUES (
				gen_random_uuid(), $1, 't1', 'src', 'ext1', gen_random_uuid(),
				'inc_01', 'fl1', 'H4_CONSUMER', 'SEV0', 'action', '/c-ctl', 'sre'
			);
		`, pubID)
		require.NoError(t, err)

		_, errUpdate := db.ExecContext(ctx, `UPDATE ops_incident_triage_log SET severity = 'SEV2' WHERE public_id = $1;`, pubID)
		require.Error(t, errUpdate)
		assert.Contains(t, errUpdate.Error(), "ops_incident_triage_log is strictly immutable")
	})
}

func TestEventOpsDashboard_TracingCompliance(t *testing.T) {
	db := getOpsTestDB(t)
	defer db.Close()
	ctx := context.Background()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)

	repo := adapters.NewPostgresEventOperationsDashboardRepository(db)
	svc := service.NewEventOperationsDashboardService(repo)

	tenantID := fmt.Sprintf("tenant_ops_tr_%d", time.Now().UnixNano())
	_, err := svc.ReportHopTelemetry(ctx, &contracts.ReportHopTelemetryInput{
		TenantID:      tenantID,
		SourceService: "order-service",
		ExternalID:    "ext_ops_tr",
		CorrelationID: uuid.New(),
		FlowName:      "orders_flow",
		HopCode:       contracts.HopH1Outbox,
		Tier:          contracts.Tier0,
		Status:        contracts.StatusOk,
		LagSeconds:    1.2,
		RunbookURL:    "https://wiki/runbook",
	})
	require.NoError(t, err)

	spans := exp.GetSpans()
	require.NotEmpty(t, spans)

	var hasReportSpan bool
	for _, s := range spans {
		if s.Name == "EventOperationsDashboardService.ReportHopTelemetry" {
			hasReportSpan = true
			break
		}
	}
	assert.True(t, hasReportSpan, "Expected EventOperationsDashboardService.ReportHopTelemetry span")
}
