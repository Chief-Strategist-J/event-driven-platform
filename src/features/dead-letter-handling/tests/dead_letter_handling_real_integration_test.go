package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/adapters"
	"event-driven-platform/src/features/dead-letter-handling/service"
	"event-driven-platform/src/shared/utils"
)

func getDLQIntegrationDB(t *testing.T) *sql.DB {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	pass := os.Getenv("DB_PASSWORD")
	if pass == "" {
		pass = "postgres"
	}
	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "event_store"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("Skipping integration test: cannot connect to real PostgreSQL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Skipping integration test: PostgreSQL unreachable: %v", err)
	}

	return db
}

func TestDeadLetterHandling_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getDLQIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	badID, _ := utils.GenerateUUIDv7()

	_, err := db.ExecContext(ctx, `
		INSERT INTO dead_letter_quarantine (
			id, public_id, tenant_id, source_service, external_id, correlation_id, topic, partition, source_offset,
			consumer_group, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
			payload, failure_reason, error_code, error_fingerprint, status
		) VALUES (
			$1, 'bad_prefix_123', 'tenant_ck', 'srv', 'ext', $1, 'topic', 0, 0,
			'grp', 'hnd', $1, 'order', 'ord', 1, 'evt',
			'{}'::jsonb, 'reason', 'ERR', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'QUARANTINED'
		);`, badID)

	if err == nil {
		t.Fatalf("expected physical check constraint violation for malformed public_id, but insert succeeded")
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO dead_letter_quarantine (
			id, public_id, tenant_id, source_service, external_id, correlation_id, topic, partition, source_offset,
			consumer_group, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
			payload, failure_reason, error_code, error_fingerprint, status
		) VALUES (
			$1, 'dlq_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext', $1, 'topic', 0, 0,
			'grp', 'hnd', $1, 'order', 'ord', 0, 'evt',
			'{}'::jsonb, 'reason', 'ERR', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'QUARANTINED'
		);`, badID)

	if err == nil {
		t.Fatalf("expected physical check constraint violation for aggregate_version < 1, but insert succeeded")
	}
}

func TestDeadLetterHandling_Integration_PostgresQuarantineAndRedrive(t *testing.T) {
	db := getDLQIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	adapter := adapters.NewPostgresDeadLetterRepositoryAdapter(db)
	svc := service.NewDeadLetterService(adapter)

	tenantID := fmt.Sprintf("tenant_dlq_%d", time.Now().UnixNano())
	aggregateID := fmt.Sprintf("ord_%d", time.Now().UnixNano())
	eventUUID, _ := utils.GenerateUUIDv7()

	input := contracts.QuarantineEventInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		ExternalID:       fmt.Sprintf("ext_%d", time.Now().UnixNano()),
		CorrelationID:    eventUUID,
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     100,
		ConsumerGroup:    "order-fulfillment-group",
		HandlerID:        "inventory-handler",
		EventID:          eventUUID,
		AggregateType:    "order",
		AggregateID:      aggregateID,
		AggregateVersion: 2,
		EventType:        "OrderCreated",
		Payload:          []byte(`{"order_id":"123","invalid":true}`),
		Headers:          map[string]string{"traceparent": "00-test-01"},
		FailureReason:    "schema violation: unknown attribute",
		ErrorCode:        "ERR_SCHEMA_VIOLATION",
	}

	rec, err := svc.QuarantineEvent(ctx, input)
	if err != nil {
		t.Fatalf("failed to quarantine event in real PostgreSQL: %v", err)
	}

	if rec.Status != contracts.StatusQuarantined {
		t.Fatalf("expected QUARANTINED status, got %s", rec.Status)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dead_letter_quarantine WHERE tenant_id = $1 AND id = $2", tenantID, rec.ID).Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("expected 1 row in dead_letter_quarantine, got %d (err: %v)", count, err)
	}

	isParked, err := svc.IsKeyParked(ctx, tenantID, "inventory-handler", "order", aggregateID)
	if err != nil || !isParked {
		t.Fatalf("expected aggregate %s to be parked in real DB, got %v", aggregateID, isParked)
	}

	invoker := func(_ context.Context, _ contracts.DeadLetterQuarantineRecord) (bool, error) {
		return true, nil
	}
	result, redriveErr := svc.RedriveEvent(ctx, tenantID, rec.ID, invoker)
	if redriveErr != nil || result == nil || !result.Success {
		t.Fatalf("failed to redrive quarantined event: %v (result: %+v)", redriveErr, result)
	}

	isParkedAfter, err := svc.IsKeyParked(ctx, tenantID, "inventory-handler", "order", aggregateID)
	if err != nil || isParkedAfter {
		t.Fatalf("expected aggregate to be unparked after redrive resolution, got %v", isParkedAfter)
	}
}
