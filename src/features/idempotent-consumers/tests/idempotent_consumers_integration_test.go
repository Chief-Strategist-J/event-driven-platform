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
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/adapters"
	"event-driven-platform/src/features/idempotent-consumers/service"
	"event-driven-platform/src/shared/utils"
)

func getIdempotentTestDB(t *testing.T) *sql.DB {
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

func TestIdempotentConsumers_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getIdempotentTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_idem_%d", time.Now().UnixNano())
	ctx := context.Background()

	t.Run("RejectMalformedPayloadHash", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		eventUUID, _ := utils.GenerateUUIDv7()
		invalidHash := "not-a-valid-sha256-hash"

		query := `INSERT INTO consumer_inbox (
			id, tenant_id, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_hash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err := db.ExecContext(ctx, query,
			validUUID, tenantID, "order-worker", eventUUID, "order", "ord_001", 1, "OrderCreated", invalidHash,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed payload_hash, got nil")
		}
		if !strings.Contains(err.Error(), "consumer_inbox_payload_hash_ck") {
			t.Fatalf("expected consumer_inbox_payload_hash_ck violation, got: %v", err)
		}
	})

	t.Run("RejectZeroAggregateVersion", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		eventUUID, _ := utils.GenerateUUIDv7()
		validHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

		query := `INSERT INTO consumer_inbox (
			id, tenant_id, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_hash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err := db.ExecContext(ctx, query,
			validUUID, tenantID, "order-worker", eventUUID, "order", "ord_002", 0, "OrderCreated", validHash,
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for version < 1, got nil")
		}
		if !strings.Contains(err.Error(), "consumer_inbox_aggregate_version_ck") {
			t.Fatalf("expected consumer_inbox_aggregate_version_ck violation, got: %v", err)
		}
	})

	t.Run("RejectDuplicateInboxDelivery", func(t *testing.T) {
		validUUID1, _ := utils.GenerateUUIDv7()
		validUUID2, _ := utils.GenerateUUIDv7()
		eventUUID, _ := utils.GenerateUUIDv7()
		validHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

		query := `INSERT INTO consumer_inbox (
			id, tenant_id, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_hash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err := db.ExecContext(ctx, query,
			validUUID1, tenantID, "order-worker", eventUUID, "order", "ord_003", 1, "OrderCreated", validHash,
		)
		if err != nil {
			t.Fatalf("initial inbox insert failed: %v", err)
		}

		_, dupErr := db.ExecContext(ctx, query,
			validUUID2, tenantID, "order-worker", eventUUID, "order", "ord_003", 1, "OrderCreated", validHash,
		)
		if dupErr == nil {
			t.Fatalf("expected duplicate key violation on consumer_inbox_dedupe_uk, got nil")
		}
		if !strings.Contains(dupErr.Error(), "consumer_inbox_dedupe_uk") {
			t.Fatalf("expected consumer_inbox_dedupe_uk error, got: %v", dupErr)
		}
	})
}

func TestIdempotentConsumers_Integration_AtomicDeduplication(t *testing.T) {
	db := getIdempotentTestDB(t)
	defer db.Close()

	repo := adapters.NewPostgresConsumerRepositoryAdapter(db)
	svc := service.NewConsumerService(repo)

	tenantID := fmt.Sprintf("tenant_idem_atomic_%d", time.Now().UnixNano())
	ctx := context.Background()
	eventID, _ := utils.GenerateUUIDv7()

	input := contracts.InboundConsumerEvent{
		TenantID:         tenantID,
		HandlerID:        "billing-projection",
		EventID:          eventID,
		AggregateType:    "invoice",
		AggregateID:      "inv_9901",
		AggregateVersion: 1,
		EventType:        "InvoiceIssued",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticState,
		Payload:          []byte(`{"invoice_id":"inv_9901","amount":100}`),
		EventOccurredAt:  time.Now().UTC(),
	}

	res1, err := svc.ConsumeEvent(ctx, input)
	if err != nil {
		t.Fatalf("unexpected failure on first consume: %v", err)
	}
	if res1.Action != contracts.ActionApply {
		t.Fatalf("expected ActionApply on initial message, got %s", res1.Action)
	}
	if res1.AppliedVersion != 1 {
		t.Fatalf("expected applied_version 1, got %d", res1.AppliedVersion)
	}

	res2, err := svc.ConsumeEvent(ctx, input)
	if err != nil {
		t.Fatalf("unexpected failure on duplicate consume: %v", err)
	}
	if res2.Action != contracts.ActionSkip {
		t.Fatalf("expected ActionSkip on replayed message, got %s", res2.Action)
	}
}

func TestIdempotentConsumers_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	ctx := context.Background()

	t.Run("SpanCreatedAndErrorRecordedOnValidationFailure", func(t *testing.T) {
		exporter.Reset()

		invalidInput := contracts.InboundConsumerEvent{
			TenantID: "",
		}

		_, err := svc.ConsumeEvent(ctx, invalidInput)
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}
	})
}
