package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/adapters"
	"event-driven-platform/src/features/event-ordering-protection/service"
	"event-driven-platform/src/shared/utils"
)

func getOrderingIntegrationDB(t *testing.T) *sql.DB {
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

func TestEventOrderingProtection_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getOrderingIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO aggregate_sequence_tracker (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				consumer_group, handler_id, aggregate_type, aggregate_id,
				last_applied_version, last_payload_hash, owner_epoch
			) VALUES (
				$1, 'bad_prefix_123', 'tenant_ck', 'srv', 'ext_01', $1,
				'grp_1', 'hnd_1', 'order', 'ord_01',
				1, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 1
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, but insert succeeded")
		}
	})

	t.Run("RejectInvalidHoldReasonEnum", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO aggregate_sequence_tracker (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				consumer_group, handler_id, aggregate_type, aggregate_id,
				last_applied_version, last_payload_hash, owner_epoch, is_held, hold_reason
			) VALUES (
				$1, 'seq_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext_02', $1,
				'grp_1', 'hnd_1', 'order', 'ord_02',
				1, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 1,
				true, 'UNKNOWN_REASON'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for invalid hold_reason enum, but insert succeeded")
		}
	})
}

func TestEventOrderingProtection_Integration_ImmutabilityTriggers(t *testing.T) {
	db := getOrderingIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()
	publicID := "seq_0192e2b08c2979a0971084519a"
	tenantID := fmt.Sprintf("tenant_imm_%d", time.Now().UnixNano())

	_, err := db.ExecContext(ctx, `
		INSERT INTO aggregate_sequence_tracker (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			consumer_group, handler_id, aggregate_type, aggregate_id,
			last_applied_version, last_payload_hash, owner_epoch
		) VALUES (
			$1, $2, $3, 'srv', 'ext_imm', $1,
			'grp_imm', 'hnd_imm', 'order', 'ord_imm',
			1, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 1
		);`, testUUID, publicID, tenantID)
	if err != nil {
		t.Fatalf("failed inserting baseline tracker: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE aggregate_sequence_tracker
		SET public_id = 'seq_0192e2b08c2979a0971084519b'
		WHERE id = $1 AND tenant_id = $2;
	`, testUUID, tenantID)

	if err == nil {
		t.Fatalf("expected immutability trigger exception on updating public_id, but update succeeded")
	}
}

func TestEventOrderingProtection_Integration_PostgresSequenceHoldAndDrain(t *testing.T) {
	db := getOrderingIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	repo := adapters.NewPostgresEventOrderingAdapter(db)
	svc := service.NewEventOrderingService(repo)

	tenantID := fmt.Sprintf("tenant_seq_%d", time.Now().UnixNano())
	group := "billing-consumer-group"
	handler := "billing-handler"
	aggType := "invoice"
	aggID := fmt.Sprintf("inv_%d", time.Now().UnixNano())

	res1, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
		TenantID:         tenantID,
		SourceService:    "invoice-service",
		ExternalID:       "ext_1",
		ConsumerGroup:    group,
		HandlerID:        handler,
		AggregateType:    aggType,
		AggregateID:      aggID,
		AggregateVersion: 1,
		EventID:          "evt_1",
		EventType:        "InvoiceGenerated",
		Payload:          []byte(`{"total": 100}`),
		OwnerEpoch:       1,
	})
	if err != nil || res1.Action != contracts.ActionApply {
		t.Fatalf("expected ActionApply on v1, got action: %s, err: %v", res1.Action, err)
	}

	res3, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
		TenantID:         tenantID,
		SourceService:    "invoice-service",
		ExternalID:       "ext_3",
		ConsumerGroup:    group,
		HandlerID:        handler,
		AggregateType:    aggType,
		AggregateID:      aggID,
		AggregateVersion: 3,
		EventID:          "evt_3",
		EventType:        "InvoiceSent",
		Payload:          []byte(`{"sent": true}`),
		OwnerEpoch:       1,
	})
	if err != nil || res3.Action != contracts.ActionGapHeld || !res3.IsHeld {
		t.Fatalf("expected ActionGapHeld on out-of-order v3, got: %+v, err: %v", res3, err)
	}

	var heldCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sequence_held_event 
		WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_id = $4;
	`, tenantID, group, handler, aggID).Scan(&heldCount)
	if err != nil || heldCount != 1 {
		t.Fatalf("expected 1 held event in PostgreSQL, got %d (err: %v)", heldCount, err)
	}

	res2, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
		TenantID:         tenantID,
		SourceService:    "invoice-service",
		ExternalID:       "ext_2",
		ConsumerGroup:    group,
		HandlerID:        handler,
		AggregateType:    aggType,
		AggregateID:      aggID,
		AggregateVersion: 2,
		EventID:          "evt_2",
		EventType:        "InvoiceApproved",
		Payload:          []byte(`{"approved": true}`),
		OwnerEpoch:       1,
	})
	if err != nil || res2.Action != contracts.ActionApply {
		t.Fatalf("expected ActionApply on arriving v2, got: %+v, err: %v", res2, err)
	}

	drainRes, err := svc.DrainHeldEvents(ctx, contracts.DrainHeldEventsInput{
		TenantID:      tenantID,
		ConsumerGroup: group,
		HandlerID:     handler,
		AggregateType: aggType,
		AggregateID:   aggID,
		OwnerEpoch:    1,
	})
	if err != nil {
		t.Fatalf("failed draining held events in real DB: %v", err)
	}
	if drainRes.EventsDrained != 1 || !drainRes.IsUnheld || drainRes.NewVersion != 3 {
		t.Fatalf("unexpected drain result: %+v", drainRes)
	}

	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sequence_held_event 
		WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_id = $4;
	`, tenantID, group, handler, aggID).Scan(&heldCount)
	if err != nil || heldCount != 0 {
		t.Fatalf("expected 0 held events remaining after drain, got %d", heldCount)
	}
}
