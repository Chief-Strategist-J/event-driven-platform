package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/adapters"
	"event-driven-platform/src/features/partitioning-strategy/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func getPartitionTestDB(t *testing.T) *sql.DB {
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

func TestPartitionIntegration_PhysicalCheckConstraints(t *testing.T) {
	db := getPartitionTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_prt_ck_" + uuid.New().String()[:8]

	t.Run("RejectMalformedTopologyPublicID", func(t *testing.T) {
		id := uuid.New()
		malformedPublicID := "invalid_id_format"
		query := `INSERT INTO partition_topic_topology (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			topic_name, topic_class, partition_count, replication_factor, min_insync_replicas
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err := db.ExecContext(ctx, query,
			id, malformedPublicID, tenantID, "order-service", "ext_01", uuid.New(),
			"topic.orders", "K_ORDERED", 16, 3, 2,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_partition_topic_topology_public_id_format") {
			t.Fatalf("expected ck_partition_topic_topology_public_id_format violation, got: %v", err)
		}
	})

	t.Run("RejectZeroPartitionCount", func(t *testing.T) {
		id := uuid.New()
		publicID := "prt_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO partition_topic_topology (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			topic_name, topic_class, partition_count, replication_factor, min_insync_replicas
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_02", uuid.New(),
			"topic.zero", "K_ORDERED", 0, 3, 2,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for partition_count=0, got nil")
		}
		if !strings.Contains(err.Error(), "ck_partition_topic_topology_partition_count") {
			t.Fatalf("expected ck_partition_topic_topology_partition_count violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidMigrationPhase", func(t *testing.T) {
		id := uuid.New()
		publicID := "pmg_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20]
		query := `INSERT INTO partition_migration_runbook_log (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			migration_id, source_topic, target_topic, source_partitions, target_partitions, phase
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_03", uuid.New(),
			"mig_invalid", "orders.v1", "orders.v2", 16, 32, "INVALID_PHASE_STATE",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid phase, got nil")
		}
		if !strings.Contains(err.Error(), "ck_partition_migration_runbook_log_phase") {
			t.Fatalf("expected ck_partition_migration_runbook_log_phase violation, got: %v", err)
		}
	})
}

func TestPartitionIntegration_PostgresLifecycle(t *testing.T) {
	db := getPartitionTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := adapters.NewPartitionPostgresAdapter(db)
	tenantID := "tenant_part_" + uuid.New().String()[:8]
	topicName := "topic_" + uuid.New().String()[:8]

	top := contracts.TopicTopology{
		ID:                    uuid.New(),
		PublicID:              "prt_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20],
		TenantID:              tenantID,
		SourceService:         "order-service",
		ExternalID:            "ext_" + uuid.New().String()[:8],
		CorrelationID:         uuid.New(),
		TopicName:             topicName,
		TopicClass:            contracts.TopicClassOrdered,
		PartitionCount:        12,
		ReplicationFactor:     3,
		MinInSyncReplicas:     2,
		AllowInPlaceExpansion: false,
		Version:               1,
		CreatedAt:             time.Now().UTC(),
		UpdatedAt:             time.Now().UTC(),
	}

	if err := adapter.SaveTopicTopology(ctx, top); err != nil {
		t.Fatalf("failed saving topic topology: %v", err)
	}

	fetched, err := adapter.GetTopicTopology(ctx, tenantID, topicName)
	if err != nil {
		t.Fatalf("failed fetching topic topology: %v", err)
	}
	if fetched.TopicName != topicName {
		t.Fatalf("expected topic %s, got %s", topicName, fetched.TopicName)
	}

	rec := contracts.MigrationRunbookRecord{
		ID:               uuid.New(),
		PublicID:         "pmg_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:20],
		TenantID:         tenantID,
		SourceService:    "order-service",
		ExternalID:       "ext_mig_" + uuid.New().String()[:8],
		CorrelationID:    uuid.New(),
		MigrationID:      "mig_" + uuid.New().String()[:8],
		SourceTopic:      topicName,
		TargetTopic:      topicName + ".v2",
		SourcePartitions: 12,
		TargetPartitions: 24,
		Phase:            contracts.MigrationPhasePlanned,
		Version:          1,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if err := adapter.CreateMigrationRunbook(ctx, rec); err != nil {
		t.Fatalf("failed creating migration runbook: %v", err)
	}

	if err := adapter.UpdateMigrationPhase(ctx, tenantID, rec.MigrationID, contracts.MigrationPhaseProvisioningTarget, 1); err != nil {
		t.Fatalf("failed advancing migration phase: %v", err)
	}

	fetchedRec, err := adapter.GetMigrationRunbook(ctx, tenantID, rec.MigrationID)
	if err != nil {
		t.Fatalf("failed fetching migration runbook: %v", err)
	}
	if fetchedRec.Phase != contracts.MigrationPhaseProvisioningTarget {
		t.Fatalf("expected phase PROVISIONING_TARGET, got %v", fetchedRec.Phase)
	}
}

func TestPartitioning_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	db := getPartitionTestDB(t)
	defer db.Close()

	repo := adapters.NewPartitionPostgresAdapter(db)
	hasher := adapters.NewMurmur2HasherAdapter()
	svc := service.NewPartitioningService(repo, hasher)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.RegisterTopicTopologyInput{
			TenantID: "",
		}

		_, err := svc.RegisterTopicTopology(ctx, invalidInput)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		rootSpan := spans[0]
		if rootSpan.Name != "partitioning.register_topic_topology" {
			t.Fatalf("expected span name 'partitioning.register_topic_topology', got '%s'", rootSpan.Name)
		}

		if rootSpan.Status.Code.String() != "Error" {
			t.Fatalf("expected span status Error, got %s", rootSpan.Status.Code.String())
		}

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

	t.Run("SpanAttributesRecordedOnSuccessfulCalculation", func(t *testing.T) {
		exporter.Reset()

		validInput := contracts.CalculatePartitionInput{
			TenantID:       "tenant-trace-test",
			TopicName:      "orders.partition.trace",
			TopicClass:     contracts.TopicClassOrdered,
			PartitionCount: 16,
			AggregateType:  "Order",
			AggregateID:    "ord_9901",
		}

		out, err := svc.CalculatePartition(ctx, validInput)
		if err != nil {
			t.Fatalf("unexpected error calculating partition: %v", err)
		}

		if out.Partition < 0 || out.Partition >= 16 {
			t.Fatalf("unexpected partition: %d", out.Partition)
		}

		spans := exporter.GetSpans()
		if len(spans) == 0 {
			t.Fatalf("expected OpenTelemetry span to be recorded, found none")
		}

		rootSpan := spans[0]
		if rootSpan.Name != "partitioning.calculate_partition" {
			t.Fatalf("expected span name 'partitioning.calculate_partition', got '%s'", rootSpan.Name)
		}

		hasResolvedAttr := false
		for _, attr := range rootSpan.Attributes {
			if string(attr.Key) == "resolved_partition" && attr.Value.AsInt64() == int64(out.Partition) {
				hasResolvedAttr = true
				break
			}
		}
		if !hasResolvedAttr {
			t.Fatalf("expected resolved_partition attribute on span, attributes: %+v", rootSpan.Attributes)
		}
	})
}
