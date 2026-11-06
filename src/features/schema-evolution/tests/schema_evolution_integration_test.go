package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/schema-evolution/contracts"
	"event-driven-platform/src/features/schema-evolution/domain/rules"
	"event-driven-platform/src/features/schema-evolution/infrastructure/adapters"
	"event-driven-platform/src/features/schema-evolution/service"
)

func getSETestDB(t *testing.T) *sql.DB {
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

func cleanHex(u uuid.UUID) string {
	return strings.ReplaceAll(u.String(), "-", "")
}

func TestSchemaEvolution_PhysicalCheckConstraints(t *testing.T) {
	db := getSETestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_se_ck_" + cleanHex(uuid.New())[:8]

	t.Run("RejectMalformedVersionPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO schema_evolution_version_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			subject_name, schema_version, compatibility_mode, schema_fingerprint,
			schema_definition, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_sev_id", tenantID, "schema-service", "ext_01", uuid.New(),
			"orders.placed", 1, "FULL_TRANSITIVE", strings.Repeat("a", 64),
			"{}", "ACTIVE",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_version_public_id_format")
	})

	t.Run("RejectNonPositiveSchemaVersion", func(t *testing.T) {
		id := uuid.New()
		publicID := "sev_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO schema_evolution_version_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			subject_name, schema_version, compatibility_mode, schema_fingerprint,
			schema_definition, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "schema-service", "ext_02", uuid.New(),
			"orders.placed", 0, "FULL_TRANSITIVE", strings.Repeat("a", 64),
			"{}", "ACTIVE",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_version_pos")
	})

	t.Run("RejectMalformedConsumerPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO schema_evolution_consumer_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			consumer_group, subject_name, min_supported_version, max_supported_version,
			run_cadence, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_sec_id", tenantID, "consumer-service", "ext_03", uuid.New(),
			"cg-orders", "orders.placed", 1, 2, "CONTINUOUS", "ACTIVE",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_consumer_public_id_format")
	})

	t.Run("RejectInvalidConsumerVersionRange", func(t *testing.T) {
		id := uuid.New()
		publicID := "sec_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO schema_evolution_consumer_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			consumer_group, subject_name, min_supported_version, max_supported_version,
			run_cadence, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "consumer-service", "ext_04", uuid.New(),
			"cg-orders", "orders.placed", 3, 2, "CONTINUOUS", "ACTIVE",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_consumer_versions_valid")
	})

	t.Run("RejectMalformedDualPublishPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO schema_evolution_dual_publish_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			subject_name, legacy_topic, modern_topic, sunset_deadline, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_sed_id", tenantID, "pub-service", "ext_05", uuid.New(),
			"orders.placed", "orders-v1", "orders-v2", time.Now().Add(24*time.Hour), "ACTIVE",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_dual_publish_public_id_format")
	})

	t.Run("RejectMalformedIncidentPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO schema_evolution_incident_log (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			subject_name, consumer_group, incident_class, payload_fingerprint,
			error_message, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_sei_id", tenantID, "incident-service", "ext_06", uuid.New(),
			"orders.placed", "cg-orders", "SINGLE_CORRUPT", strings.Repeat("c", 64),
			"error reading schema", "OPEN",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_schema_evolution_incident_public_id_format")
	})
}

func TestSchemaEvolution_PostgresLifecycleAndGating(t *testing.T) {
	db := getSETestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresSchemaEvolutionRepositoryAdapter(db)
	svc := service.NewSchemaEvolutionService(repo)

	tenantID := "tenant_se_life_" + cleanHex(uuid.New())[:8]
	subject := "orders.created." + cleanHex(uuid.New())[:6]

	v1Input := contracts.RegisterSchemaInput{
		TenantID:          tenantID,
		SourceService:     "schema-service",
		ExternalID:        "ext_sch_01",
		CorrelationID:     uuid.New().String(),
		SubjectName:       subject,
		SchemaVersion:     1,
		CompatibilityMode: contracts.CompatibilityFullTransitive,
		SchemaDefinition:  `{"type":"record","name":"OrderCreated","fields":[{"name":"order_id","type":"string"}]}`,
	}
	v1, err := svc.RegisterSchema(ctx, v1Input)
	require.NoError(t, err)
	assert.NotEmpty(t, v1.PublicID)
	assert.Equal(t, 1, v1.SchemaVersion)

	_, errDup := svc.RegisterSchema(ctx, contracts.RegisterSchemaInput{
		TenantID:          tenantID,
		SourceService:     "schema-service",
		ExternalID:        "ext_sch_02",
		CorrelationID:     uuid.New().String(),
		SubjectName:       subject,
		SchemaVersion:     2,
		CompatibilityMode: contracts.CompatibilityFullTransitive,
		SchemaDefinition:  v1Input.SchemaDefinition,
	})
	require.Error(t, errDup)
	assert.Equal(t, service.ErrDuplicateSchemaFingerprint, errDup)

	consumerGroup := "cg-order-projection"
	cRec, err := svc.RegisterConsumerSupport(ctx, tenantID, consumerGroup, subject, 1, 1)
	require.NoError(t, err)
	assert.NotEmpty(t, cRec.PublicID)

	readiness1, err := svc.CheckConsumerReadiness(ctx, contracts.CheckConsumerReadinessInput{
		TenantID:      tenantID,
		SubjectName:   subject,
		TargetVersion: 2,
	})
	require.NoError(t, err)
	assert.False(t, readiness1.IsReady)
	assert.Contains(t, readiness1.UnreadyConsumers, consumerGroup)

	_, err = svc.RegisterConsumerSupport(ctx, tenantID, consumerGroup, subject, 1, 2)
	require.NoError(t, err)

	readiness2, err := svc.CheckConsumerReadiness(ctx, contracts.CheckConsumerReadinessInput{
		TenantID:      tenantID,
		SubjectName:   subject,
		TargetVersion: 2,
	})
	require.NoError(t, err)
	assert.True(t, readiness2.IsReady)
	assert.Empty(t, readiness2.UnreadyConsumers)

	dpInput := contracts.InitiateDualPublishInput{
		TenantID:       tenantID,
		SourceService:  "schema-service",
		CorrelationID:  uuid.New().String(),
		SubjectName:    subject,
		LegacyTopic:    "orders-v1-topic",
		ModernTopic:    "orders-v2-topic",
		SunsetDeadline: time.Now().Add(7 * 24 * time.Hour),
	}
	dpRec, err := svc.InitiateDualPublish(ctx, dpInput)
	require.NoError(t, err)
	assert.NotEmpty(t, dpRec.PublicID)

	incInput := contracts.RecordIncidentInput{
		TenantID:           tenantID,
		SourceService:      "consumer-guard",
		CorrelationID:      uuid.New().String(),
		SubjectName:        subject,
		ConsumerGroup:      consumerGroup,
		IncidentClass:      contracts.IncidentSingleCorrupt,
		PayloadFingerprint: rules.ComputeSchemaFingerprint(`{"corrupt":true}`),
		ErrorMessage:       "deserialization error: unknown field",
	}
	incRec, err := svc.RecordEvolutionIncident(ctx, incInput)
	require.NoError(t, err)
	assert.NotEmpty(t, incRec.PublicID)
	assert.Equal(t, "OPEN", incRec.Status)
}

func TestSchemaEvolution_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	db := getSETestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresSchemaEvolutionRepositoryAdapter(db)
	svc := service.NewSchemaEvolutionService(repo)

	tenantID := "tenant_se_trace_" + cleanHex(uuid.New())[:8]
	subject := "orders.trace." + cleanHex(uuid.New())[:6]

	_, err := svc.RegisterSchema(ctx, contracts.RegisterSchemaInput{
		TenantID:          tenantID,
		SourceService:     "trace-suite",
		ExternalID:        "ext_sch_trace",
		CorrelationID:     uuid.New().String(),
		SubjectName:       subject,
		SchemaVersion:     1,
		CompatibilityMode: contracts.CompatibilityFullTransitive,
		SchemaDefinition:  `{"type":"record","name":"TraceOrder","fields":[{"name":"id","type":"string"}]}`,
	})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	var hasServiceSpan, hasRepoSpan bool
	for _, span := range spans {
		if span.Name == "SchemaEvolutionService.RegisterSchema" {
			hasServiceSpan = true
		}
		if span.Name == "PostgresSchemaEvolutionRepositoryAdapter.RegisterSchemaVersion" ||
			span.Name == "PostgresSchemaEvolutionRepositoryAdapter.GetSchemaLineage" {
			hasRepoSpan = true
		}
	}

	assert.True(t, hasServiceSpan, "expected SchemaEvolutionService.RegisterSchema span in trace exporter")
	assert.True(t, hasRepoSpan, "expected PostgresSchemaEvolutionRepositoryAdapter span in trace exporter")
}
