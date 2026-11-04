package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/infrastructure/adapters"
	"event-driven-platform/src/features/retry-topics/service"
	"event-driven-platform/src/shared/utils"
)

func getRetryIntegrationDB(t *testing.T) *sql.DB {
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


func TestRetryPolicy_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getRetryIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {

		_, err := db.ExecContext(ctx, `
			INSERT INTO retry_attempt_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				consumer_group, handler_id, event_id, aggregate_type, aggregate_id,
				aggregate_version, attempt_number, max_attempts, backoff_ms, due_at,
				status, failure_classification, idempotency_key
			) VALUES (
				$1, 'bad_prefix_123', 'tenant_ck', 'srv', 'ext_01', $1,
				'grp_1', 'hnd_1', 'evt_1', 'order', 'ord_01',
				1, 1, 3, 1000, NOW(),
				'SCHEDULED', 'MESSAGE_TRANSIENT', 'idemp_01'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, but insert succeeded")
		}
	})

	t.Run("RejectInvalidStatusEnum", func(t *testing.T) {

		_, err := db.ExecContext(ctx, `
			INSERT INTO retry_attempt_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				consumer_group, handler_id, event_id, aggregate_type, aggregate_id,
				aggregate_version, attempt_number, max_attempts, backoff_ms, due_at,
				status, failure_classification, idempotency_key
			) VALUES (
				$1, 'try_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext_02', $1,
				'grp_1', 'hnd_1', 'evt_2', 'order', 'ord_02',
				1, 1, 3, 1000, NOW(),
				'UNKNOWN_STATUS', 'MESSAGE_TRANSIENT', 'idemp_02'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid status, but insert succeeded")
		}
	})

	t.Run("RejectAttemptNumberExceedingMaxAttempts", func(t *testing.T) {

		_, err := db.ExecContext(ctx, `
			INSERT INTO retry_attempt_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				consumer_group, handler_id, event_id, aggregate_type, aggregate_id,
				aggregate_version, attempt_number, max_attempts, backoff_ms, due_at,
				status, failure_classification, idempotency_key
			) VALUES (
				$1, 'try_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext_03', $1,
				'grp_1', 'hnd_1', 'evt_3', 'order', 'ord_03',
				1, 4, 3, 1000, NOW(),
				'SCHEDULED', 'MESSAGE_TRANSIENT', 'idemp_03'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for attempt_number > max_attempts, but insert succeeded")
		}
	})
}


func TestRetryPolicy_Integration_ImmutabilityTriggers(t *testing.T) {
	db := getRetryIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()
	publicID := "try_0192e2b08c2979a0971084519a"
	tenantID := fmt.Sprintf("tenant_imm_%d", time.Now().UnixNano())


	_, err := db.ExecContext(ctx, `
		INSERT INTO retry_attempt_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			consumer_group, handler_id, event_id, aggregate_type, aggregate_id,
			aggregate_version, attempt_number, max_attempts, backoff_ms, due_at,
			status, failure_classification, idempotency_key
		) VALUES (
			$1, $2, $3, 'srv', 'ext_imm', $1,
			'grp_imm', 'hnd_imm', 'evt_imm', 'order', 'ord_imm',
			1, 1, 3, 1000, NOW(),
			'SCHEDULED', 'MESSAGE_TRANSIENT', 'idemp_imm'
		);`, testUUID, publicID, tenantID)
	if err != nil {
		t.Fatalf("failed inserting baseline retry record: %v", err)
	}


	_, err = db.ExecContext(ctx, `
		UPDATE retry_attempt_ledger
		SET public_id = 'try_0192e2b08c2979a0971084519b'
		WHERE id = $1 AND tenant_id = $2;
	`, testUUID, tenantID)

	if err == nil {
		t.Fatalf("expected immutability trigger exception on updating public_id, but update succeeded")
	}
}


func TestRetryPolicy_Integration_PostgresAdapterScheduleAndPoll(t *testing.T) {
	db := getRetryIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	repo := adapters.NewPostgresRetryTopicsAdapter(db)
	svc := service.NewRetryTopicsService(repo)

	tenantID := fmt.Sprintf("tenant_rt_%d", time.Now().UnixNano())
	group := "fulfillment-group"
	handler := "inventory-handler"
	eventID := fmt.Sprintf("evt_%d", time.Now().UnixNano())

	dueTime := time.Now().UTC().Add(-2 * time.Second)


	input := contracts.ScheduleRetryInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		ExternalID:       fmt.Sprintf("ext_%d", time.Now().UnixNano()),
		ConsumerGroup:    group,
		HandlerID:        handler,
		EventID:          eventID,
		AggregateType:    "order",
		AggregateID:      "ord_9901",
		AggregateVersion: 1,
		AttemptNumber:    1,
		MaxAttempts:      3,
		BackoffMs:        1000,
		DueAt:            dueTime,
		Classification:   contracts.ClassMessageTransient,
		ErrorMessage:     "remote inventory service timeout",
		ErrorCode:        "ERR_TIMEOUT",
	}

	rec, err := svc.ScheduleRetryAttempt(ctx, input)
	if err != nil {
		t.Fatalf("failed scheduling retry attempt in PostgreSQL: %v", err)
	}
	if rec.Status != contracts.StatusScheduled {
		t.Fatalf("expected SCHEDULED status, got %s", rec.Status)
	}


	_, err = svc.ScheduleRetryAttempt(ctx, input)
	if err == nil {
		t.Fatalf("expected unique constraint violation on duplicate attempt, but insert succeeded")
	}


	dueRecords, err := svc.GetDueRetries(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("failed querying due retries: %v", err)
	}
	if len(dueRecords) != 1 {
		t.Fatalf("expected 1 due retry record, got %d", len(dueRecords))
	}
	if dueRecords[0].ID != rec.ID {
		t.Fatalf("expected due record id %s, got %s", rec.ID, dueRecords[0].ID)
	}


	err = svc.MarkAttemptStatus(ctx, tenantID, rec.ID, contracts.StatusScheduled, contracts.StatusRetrying)
	if err != nil {
		t.Fatalf("failed updating status to RETRYING: %v", err)
	}


	dueAfter, err := svc.GetDueRetries(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("failed querying due retries after status change: %v", err)
	}
	if len(dueAfter) != 0 {
		t.Fatalf("expected 0 due retries after transition to RETRYING, got %d", len(dueAfter))
	}


	err = svc.MarkAttemptStatus(ctx, tenantID, rec.ID, contracts.StatusRetrying, contracts.StatusResolved)
	if err != nil {
		t.Fatalf("failed finalizing attempt as RESOLVED: %v", err)
	}
}
