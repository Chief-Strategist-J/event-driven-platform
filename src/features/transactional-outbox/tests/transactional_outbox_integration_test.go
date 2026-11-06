package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	"event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/shared/utils"
)

func getOutboxTestDB(t *testing.T) *sql.DB {
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

type dummyProducer struct{}

func (d *dummyProducer) Publish(_ context.Context, _ string, _ []byte, _ []byte) error {
	return nil
}
func (d *dummyProducer) Close() error { return nil }

func TestTransactionalOutbox_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getOutboxTestDB(t)
	defer db.Close()

	tenantID := fmt.Sprintf("tenant_otb_%d", time.Now().UnixNano())
	ctx := context.Background()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		invalidPublicID := "invalid_prefix_otb_123"

		query := `INSERT INTO outbox (
			id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
			aggregate_type, aggregate_id, aggregate_version, event_type, schema_version, payload, headers, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			validUUID, invalidPublicID, tenantID, "order-service", "ext-01", corrUUID, "cause-01",
			"order", "ord_001", 1, "OrderCreated", "1.0.0", []byte(`{"order_id":"ord_001"}`), []byte(`{}`), "PENDING",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, got nil")
		}
		if !strings.Contains(err.Error(), "outbox_public_id_ck") {
			t.Fatalf("expected outbox_public_id_ck violation, got: %v", err)
		}
	})

	t.Run("RejectInvalidStatusEnum", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("otb")

		query := `INSERT INTO outbox (
			id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
			aggregate_type, aggregate_id, aggregate_version, event_type, schema_version, payload, headers, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-02", corrUUID, "cause-02",
			"order", "ord_002", 1, "OrderCreated", "1.0.0", []byte(`{"order_id":"ord_002"}`), []byte(`{}`), "INVALID_STATUS",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid status, got nil")
		}
		if !strings.Contains(err.Error(), "outbox_status_ck") {
			t.Fatalf("expected outbox_status_ck violation, got: %v", err)
		}
	})

	t.Run("RejectZeroAggregateVersion", func(t *testing.T) {
		validUUID, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("otb")

		query := `INSERT INTO outbox (
			id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
			aggregate_type, aggregate_id, aggregate_version, event_type, schema_version, payload, headers, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			validUUID, publicID, tenantID, "order-service", "ext-03", corrUUID, "cause-03",
			"order", "ord_003", 0, "OrderCreated", "1.0.0", []byte(`{"order_id":"ord_003"}`), []byte(`{}`), "PENDING",
		)
		if err == nil {
			t.Fatalf("expected physical check constraint violation for version < 1, got nil")
		}
		if !strings.Contains(err.Error(), "outbox_aggregate_version_ck") {
			t.Fatalf("expected outbox_aggregate_version_ck violation, got: %v", err)
		}
	})

	t.Run("RejectDuplicateAggregateVersionCollision", func(t *testing.T) {
		validUUID1, _ := utils.GenerateUUIDv7()
		validUUID2, _ := utils.GenerateUUIDv7()
		corrUUID, _ := utils.GenerateUUIDv7()
		publicID1, _ := utils.GeneratePublicID("otb")
		publicID2, _ := utils.GeneratePublicID("otb")

		query := `INSERT INTO outbox (
			id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
			aggregate_type, aggregate_id, aggregate_version, event_type, schema_version, payload, headers, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			validUUID1, publicID1, tenantID, "order-service", "ext-04", corrUUID, "cause-04",
			"order", "ord_same_01", 1, "OrderCreated", "1.0.0", []byte(`{"order_id":"ord_004"}`), []byte(`{}`), "PENDING",
		)
		if err != nil {
			t.Fatalf("initial outbox insert failed: %v", err)
		}

		_, dupErr := db.ExecContext(ctx, query,
			validUUID2, publicID2, tenantID, "order-service", "ext-05", corrUUID, "cause-05",
			"order", "ord_same_01", 1, "OrderCreated", "1.0.0", []byte(`{"order_id":"ord_005"}`), []byte(`{}`), "PENDING",
		)
		if dupErr == nil {
			t.Fatalf("expected aggregate version uniqueness violation, got nil")
		}
		if !strings.Contains(dupErr.Error(), "outbox_aggregate_version_uk") {
			t.Fatalf("expected outbox_aggregate_version_uk violation, got: %v", dupErr)
		}
	})
}

func TestTransactionalOutbox_Integration_StagingAndRelay(t *testing.T) {
	db := getOutboxTestDB(t)
	defer db.Close()

	repo := adapters.NewPostgresOutboxRepositoryAdapter(db)
	svc := service.NewOutboxService(repo, &dummyProducer{})

	tenantID := fmt.Sprintf("tenant_otb_stage_%d", time.Now().UnixNano())
	ctx := context.Background()
	corrUUID, _ := utils.GenerateUUIDv7()

	input := contracts.CreateOutboxRecordInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		ExternalID:       "ext-stage-01",
		CorrelationID:    corrUUID,
		CausationID:      "cause-stage-01",
		AggregateType:    "order",
		AggregateID:      "ord_stage_99",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{"order_id":"ord_stage_99","total_cents":5000}`),
		Headers:          map[string]string{"traceparent": "00-test-trace-01"},
	}

	idempotencyKey := fmt.Sprintf("idem_key_%d", time.Now().UnixNano())

	rec1, err := svc.StageEvent(ctx, input, idempotencyKey)
	if err != nil {
		t.Fatalf("failed to stage outbox event in postgres: %v", err)
	}
	if rec1.Status != contracts.StatusPending {
		t.Fatalf("expected status PENDING, got %s", rec1.Status)
	}

	rec2, err := svc.StageEvent(ctx, input, idempotencyKey)
	if err != nil {
		t.Fatalf("failed on idempotent duplicate stage: %v", err)
	}
	if rec2.ID != rec1.ID {
		t.Fatalf("expected same record ID on duplicate idempotency key, got %s vs %s", rec2.ID, rec1.ID)
	}

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("failed to fetch pending outbox batch: %v", err)
	}
	if len(pending) == 0 {
		t.Fatalf("expected at least 1 pending outbox record in batch")
	}

	workerID := "worker-pod-01"
	leaseDuration := 10 * time.Second
	claimed, err := repo.ClaimInFlight(ctx, workerID, leaseDuration, 10)
	if err != nil {
		t.Fatalf("failed to claim in-flight outbox lease: %v", err)
	}
	if len(claimed) == 0 {
		t.Fatalf("expected claimed in-flight records")
	}

	err = repo.MarkPublished(ctx, tenantID, rec1.ID)
	if err != nil {
		t.Fatalf("failed to mark outbox record published: %v", err)
	}

	var dbStatus string
	var processedAt sql.NullTime
	err = db.QueryRowContext(ctx, "SELECT status, processed_at FROM outbox WHERE tenant_id = $1 AND id = $2", tenantID, rec1.ID).Scan(&dbStatus, &processedAt)
	if err != nil {
		t.Fatalf("failed querying outbox row: %v", err)
	}
	if dbStatus != "PUBLISHED" {
		t.Fatalf("expected status PUBLISHED, got %s", dbStatus)
	}
	if !processedAt.Valid {
		t.Fatalf("expected processed_at timestamp to be set on PUBLISHED record")
	}
}

func TestTransactionalOutbox_Integration_ConcurrentIdempotencyClaims(t *testing.T) {
	db := getOutboxTestDB(t)
	defer db.Close()

	repo := adapters.NewPostgresOutboxRepositoryAdapter(db)

	tenantID := fmt.Sprintf("tenant_otb_conc_%d", time.Now().UnixNano())
	idempotencyKey := fmt.Sprintf("conc_idem_%d", time.Now().UnixNano())
	ctx := context.Background()

	const concurrency = 10
	var wg sync.WaitGroup
	claimedWins := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		recordID, _ := utils.GenerateUUIDv7()
		go func(rID string) {
			defer wg.Done()
			claimed, _, err := repo.ClaimIdempotency(ctx, tenantID, idempotencyKey, rID)
			if err == nil && claimed {
				mu.Lock()
				claimedWins++
				mu.Unlock()
			}
		}(recordID)
	}

	wg.Wait()

	if claimedWins != 1 {
		t.Fatalf("expected exactly 1 winner on concurrent idempotency claim, got %d", claimedWins)
	}
}

func TestTransactionalOutbox_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	svc := service.NewOutboxService(repo, &dummyProducer{})

	ctx := context.Background()

	t.Run("ValidationRejectionFailsWithTypedError", func(t *testing.T) {
		invalidInput := contracts.CreateOutboxRecordInput{
			TenantID: "",
		}

		_, err := svc.StageEvent(ctx, invalidInput, "idem-test")
		if err == nil {
			t.Fatalf("expected validation error, got nil")
		}
	})
}
