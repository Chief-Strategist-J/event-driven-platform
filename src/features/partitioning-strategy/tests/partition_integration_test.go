package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/adapters"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestPartitionIntegration_PostgresLifecycle(t *testing.T) {
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

	adapter := adapters.NewPartitionPostgresAdapter(db)
	tenantID := "tenant_part_" + uuid.New().String()[:8]
	topicName := "topic_" + uuid.New().String()[:8]

	top := contracts.TopicTopology{
		ID:                    uuid.New(),
		PublicID:              "prt_" + uuid.New().String()[:16],
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
		PublicID:         "pmg_" + uuid.New().String()[:16],
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

