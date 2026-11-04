package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/infrastructure/adapters"
	"event-driven-platform/src/features/consumer-groups/service"
	"event-driven-platform/src/shared/utils"
)

func getTestDatabase(t *testing.T) *sql.DB {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
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

func TestConsumerGroups_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getTestDatabase(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_cg_%d", time.Now().UnixNano())
	ctx := context.Background()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		invalidPublicID := "invalid_prefix_123"

		query := `INSERT INTO consumer_group_member (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, member_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err := db.ExecContext(ctx, query,
			validUUID, invalidPublicID, tenantID, "order-service", "ext-01", corrUUID,
			"order-workers", "worker-1", "JOINING",
		)
		if err == nil {
			t.Fatalf("expected physical constraint violation for public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_group_member_public_id") {
			t.Fatalf("expected ck_consumer_group_member_public_id violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidStatusEnum", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("cgm")
		invalidStatus := "UNKNOWN_STATUS"

		query := `INSERT INTO consumer_group_member (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, member_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-02", corrUUID,
			"order-workers", "worker-2", invalidStatus,
		)
		if err == nil {
			t.Fatalf("expected physical constraint violation for status enum, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_group_member_status") {
			t.Fatalf("expected ck_consumer_group_member_status violation, got: %v", err)
		}
	})

	t.Run("RejectNegativeGenerationID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("cgm")

		query := `INSERT INTO consumer_group_member (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, member_id, generation_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-03", corrUUID,
			"order-workers", "worker-3", -5, "JOINING",
		)
		if err == nil {
			t.Fatalf("expected physical constraint violation for negative generation_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_group_member_generation") {
			t.Fatalf("expected ck_consumer_group_member_generation violation, got: %v", err)
		}
	})
}

func TestConsumerGroups_Integration_ImmutabilityTriggers(t *testing.T) {
	db := getTestDatabase(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_cg_%d", time.Now().UnixNano())
	ctx := context.Background()

	validUUID, _ := utils.GenerateUUIDv7()
	corrUUID, _ := utils.GenerateUUIDv7()
	publicID, _ := utils.GeneratePublicID("cgm")

	insertQuery := `INSERT INTO consumer_group_member (
		id, public_id, tenant_id, source_service, external_id, correlation_id,
		group_id, member_id, status
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := db.ExecContext(ctx, insertQuery,
		validUUID, publicID, tenantID, "order-service", "ext-mut-01", corrUUID,
		"order-workers", "worker-mut-1", "JOINING",
	)
	if err != nil {
		t.Fatalf("failed inserting baseline member record: %v", err)
	}

	// Unlawful modification of immutable public_id
	_, err = db.ExecContext(ctx,
		"UPDATE consumer_group_member SET public_id = $1 WHERE tenant_id = $2 AND id = $3",
		"cgm_tampered12345678901234", tenantID, validUUID,
	)
	if err == nil {
		t.Fatalf("expected trigger to prevent immutable public_id modification, got nil")
	}
	if !strings.Contains(err.Error(), "Immutable identifier modification rejected") {
		t.Fatalf("expected immutable trigger exception, got: %v", err)
	}
}

func TestConsumerGroups_Integration_PostgresAdapterLifecycle(t *testing.T) {
	db := getTestDatabase(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_cg_%d", time.Now().UnixNano())
	ctx := context.Background()

	adapter := adapters.NewPostgresConsumerGroupsAdapter(db)
	svc := service.NewConsumerGroupsService(adapter)

	groupID := "billing-workers-v1"
	memberID := "billing-pod-1"
	instanceID := "billing-pod-1-inst"

	// 1. Register Member
	memberRec, err := svc.RegisterMember(ctx, contracts.RegisterMemberInput{
		TenantID:           tenantID,
		SourceService:      "billing-service",
		ExternalID:         "ext-reg-01",
		CorrelationID:      "0192e2b0-8c29-79a0-9710-84519fbbd801",
		GroupID:            groupID,
		MemberID:           memberID,
		InstanceID:         &instanceID,
		ClientHost:         "10.0.0.12",
		AssignmentStrategy: contracts.CanonicalCooperativeStickyAssignor,
	})
	if err != nil {
		t.Fatalf("failed registering member via postgres adapter: %v", err)
	}
	if memberRec.Status != contracts.MemberStatusJoining {
		t.Fatalf("expected JOINING status, got %s", memberRec.Status)
	}

	// 2. Process Heartbeat -> STABLE
	heartbeatRes, err := svc.ProcessHeartbeat(ctx, contracts.HeartbeatInput{
		TenantID:   tenantID,
		GroupID:    groupID,
		MemberID:   memberID,
		InstanceID: &instanceID,
	})
	if err != nil {
		t.Fatalf("failed processing heartbeat: %v", err)
	}
	if heartbeatRes.Status != contracts.MemberStatusStable {
		t.Fatalf("expected STABLE status, got %s", heartbeatRes.Status)
	}

	// 3. Sync Rebalance
	assigned := []contracts.PartitionAssignment{
		{Topic: "billing.events", Partition: 0, Epoch: 1},
		{Topic: "billing.events", Partition: 1, Epoch: 1},
	}
	rebalanceLog, err := svc.SyncRebalance(ctx, contracts.RebalanceSyncInput{
		TenantID:           tenantID,
		SourceService:      "billing-service",
		ExternalID:         "ext-reb-01",
		CorrelationID:      "0192e2b0-8c29-79a0-9710-84519fbbd802",
		GroupID:            groupID,
		MemberID:           memberID,
		GenerationID:       1,
		RebalanceReason:    "initial assignment",
		MemberCount:        1,
		AssignedPartitions: assigned,
		DurationMs:         210,
	})
	if err != nil {
		t.Fatalf("failed syncing rebalance: %v", err)
	}
	if rebalanceLog.PublicID == "" {
		t.Fatalf("expected public ID on rebalance log")
	}

	// 4. List Members
	members, err := svc.ListMembers(ctx, tenantID, groupID)
	if err != nil {
		t.Fatalf("failed listing members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].GenerationID != 1 {
		t.Fatalf("expected generation 1, got %d", members[0].GenerationID)
	}
}
