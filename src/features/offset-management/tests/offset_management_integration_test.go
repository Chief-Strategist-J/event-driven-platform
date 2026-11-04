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

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/infrastructure/adapters"
	"event-driven-platform/src/features/offset-management/service"
	"event-driven-platform/src/shared/utils"
)

func getOffsetTestDB(t *testing.T) *sql.DB {
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

func TestOffsetManagement_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getOffsetTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_om_%d", time.Now().UnixNano())
	ctx := context.Background()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		invalidPublicID := "invalid_prefix_cso_123"

		query := `INSERT INTO consumer_stored_offset (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, topic, partition, stored_offset, log_end_offset, lag, commit_epoch
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

		_, err := db.ExecContext(ctx, query,
			validUUID, invalidPublicID, tenantID, "order-service", "ext-01", corrUUID,
			"order-workers", "orders.v1", 0, 10, 20, 10, 1,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_stored_offset_public_id") {
			t.Fatalf("expected ck_consumer_stored_offset_public_id violation, got: %v", err)
		}
	})

	t.Run("RejectNegativeStoredOffset", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("cso")

		query := `INSERT INTO consumer_stored_offset (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, topic, partition, stored_offset, log_end_offset, lag, commit_epoch
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-02", corrUUID,
			"order-workers", "orders.v1", 0, -10, 20, 30, 1,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for negative stored_offset, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_stored_offset_stored_non_neg") {
			t.Fatalf("expected ck_consumer_stored_offset_stored_non_neg violation, got: %v", err)
		}
	})

	t.Run("RejectNegativeLag", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("cso")

		query := `INSERT INTO consumer_stored_offset (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			group_id, topic, partition, stored_offset, log_end_offset, lag, commit_epoch
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-03", corrUUID,
			"order-workers", "orders.v1", 0, 50, 40, -10, 1,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for negative lag, got nil")
		}
		if !strings.Contains(err.Error(), "ck_consumer_stored_offset_lag_non_neg") {
			t.Fatalf("expected ck_consumer_stored_offset_lag_non_neg violation, got: %v", err)
		}
	})
}

func TestOffsetManagement_Integration_ImmutabilityTriggers(t *testing.T) {
	db := getOffsetTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_om_%d", time.Now().UnixNano())
	ctx := context.Background()

	validUUID, _ := utils.GenerateUUIDv7()
	corrUUID, _ := utils.GenerateUUIDv7()
	publicID, _ := utils.GeneratePublicID("cso")

	insertQuery := `INSERT INTO consumer_stored_offset (
		id, public_id, tenant_id, source_service, external_id, correlation_id,
		group_id, topic, partition, stored_offset, log_end_offset, lag, commit_epoch
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

	_, err := db.ExecContext(ctx, insertQuery,
		validUUID, publicID, tenantID, "order-service", "ext-mut-01", corrUUID,
		"order-workers", "orders.v1", 0, 10, 20, 10, 1,
	)
	if err != nil {
		t.Fatalf("failed inserting baseline stored offset: %v", err)
	}

	_, err = db.ExecContext(ctx,
		"UPDATE consumer_stored_offset SET public_id = $1 WHERE tenant_id = $2 AND id = $3",
		"cso_tampered12345678901234", tenantID, validUUID,
	)
	if err == nil {
		t.Fatalf("expected trigger to prevent immutable public_id update, got nil")
	}
	if !strings.Contains(err.Error(), "Immutable identifier modification rejected") {
		t.Fatalf("expected immutable trigger exception, got: %v", err)
	}

	auditUUID, _ := utils.GenerateUUIDv7()
	auditPubID, _ := utils.GeneratePublicID("coal")
	auditInsert := `INSERT INTO consumer_offset_audit_log (
		id, public_id, tenant_id, source_service, external_id, correlation_id,
		group_id, topic, partition, operation_type, previous_offset, target_offset, reason
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

	_, err = db.ExecContext(ctx, auditInsert,
		auditUUID, auditPubID, tenantID, "order-service", "ext-audit-01", corrUUID,
		"order-workers", "orders.v1", 0, "COMMIT", 0, 10, "Initial commit",
	)
	if err != nil {
		t.Fatalf("failed inserting audit log entry: %v", err)
	}

	_, err = db.ExecContext(ctx, "DELETE FROM consumer_offset_audit_log WHERE id = $1", auditUUID)
	if err == nil {
		t.Fatalf("expected trigger to prevent deletion of audit log, got nil")
	}
	if !strings.Contains(err.Error(), "consumer_offset_audit_log is append-only") {
		t.Fatalf("expected append-only trigger exception, got: %v", err)
	}
}

func TestOffsetManagement_Integration_PostgresAdapterLifecycle(t *testing.T) {
	db := getOffsetTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_om_%d", time.Now().UnixNano())
	ctx := context.Background()

	adapter := adapters.NewPostgresOffsetManagementAdapter(db)
	svc := service.NewOffsetManagementService(adapter)

	groupID := "shipping-processor-v1"
	topic := "shipments.dispatched"
	partition := int32(0)

	rec1, err := svc.CommitOffset(ctx, contracts.CommitOffsetInput{
		TenantID:      tenantID,
		SourceService: "shipping-service",
		ExternalID:    "ext-comm-01",
		CorrelationID: "0192e2b0-8c29-79a0-9710-84519fbbd801",
		GroupID:       groupID,
		Topic:         topic,
		Partition:     partition,
		Offset:        50,
		LogEndOffset:  100,
	})
	if err != nil {
		t.Fatalf("failed committing offset in postgres: %v", err)
	}
	if rec1.StoredOffset != 50 || rec1.CommitEpoch != 1 {
		t.Fatalf("expected offset 50 epoch 1, got offset %d epoch %d", rec1.StoredOffset, rec1.CommitEpoch)
	}

	rec2, err := svc.CommitOffset(ctx, contracts.CommitOffsetInput{
		TenantID:      tenantID,
		SourceService: "shipping-service",
		ExternalID:    "ext-comm-02",
		CorrelationID: "0192e2b0-8c29-79a0-9710-84519fbbd802",
		GroupID:       groupID,
		Topic:         topic,
		Partition:     partition,
		Offset:        80,
		LogEndOffset:  100,
	})
	if err != nil {
		t.Fatalf("failed advancing offset in postgres: %v", err)
	}
	if rec2.StoredOffset != 80 || rec2.CommitEpoch != 2 {
		t.Fatalf("expected offset 80 epoch 2, got offset %d epoch %d", rec2.StoredOffset, rec2.CommitEpoch)
	}

	rewound, err := svc.RewindOffset(ctx, contracts.RewindOffsetInput{
		TenantID:      tenantID,
		SourceService: "shipping-service",
		ExternalID:    "ext-rew-01",
		CorrelationID: "0192e2b0-8c29-79a0-9710-84519fbbd803",
		GroupID:       groupID,
		Topic:         topic,
		Partition:     partition,
		TargetOffset:  40,
		Reason:        "Replay past incident events",
	})
	if err != nil {
		t.Fatalf("failed rewinding offset: %v", err)
	}
	if rewound.StoredOffset != 40 || rewound.CommitEpoch != 3 {
		t.Fatalf("expected rewound offset 40 epoch 3, got %d epoch %d", rewound.StoredOffset, rewound.CommitEpoch)
	}

	var auditCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM consumer_offset_audit_log WHERE tenant_id = $1 AND group_id = $2",
		tenantID, groupID,
	).Scan(&auditCount)
	if err != nil {
		t.Fatalf("failed querying audit log count: %v", err)
	}
	if auditCount != 3 {
		t.Fatalf("expected 3 audit log entries (2 commits + 1 rewind), got %d", auditCount)
	}
}
