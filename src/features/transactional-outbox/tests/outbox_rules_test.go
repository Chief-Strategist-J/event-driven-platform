package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/messaging/engine/producers"
	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	"event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/shared/constants"
)

func TestOutboxStagingAndRelayWithV3Rules(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	producer := producers.NewMemoryProducerAdapter()
	svc := service.NewOutboxService(repo, producer)

	corrID, _ := uuid.NewV7()
	input := contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_corp_01",
		SourceService:    "orders-service",
		ExternalID:       "ext_ord_88192",
		CorrelationID:    corrID.String(),
		CausationID:      "cmd_checkout_991",
		AggregateType:    "order",
		AggregateID:      "ord_12345",
		AggregateVersion: 1,
		EventType:        "placed.v1",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{"order_id":"ord_12345","amount":100}`),
		Headers: map[string]string{
			constants.HeaderTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
	}

	record, err := svc.StageEvent(ctx, input, "idemp_order_12345")
	if err != nil {
		t.Fatalf("unexpected stage error: %v", err)
	}

	if record.Status != contracts.StatusPending {
		t.Errorf("expected status PENDING, got %s", record.Status)
	}
	if record.AggregateVersion != 1 {
		t.Errorf("expected aggregate version 1, got %d", record.AggregateVersion)
	}
	if record.Headers[constants.HeaderSourceService] != "orders-service" {
		t.Errorf("expected source_service header orders-service, got %s", record.Headers[constants.HeaderSourceService])
	}

	dupRecord, err := svc.StageEvent(ctx, input, "idemp_order_12345")
	if err != nil {
		t.Fatalf("unexpected error on idempotent retry: %v", err)
	}
	if dupRecord.ID != record.ID {
		t.Errorf("expected idempotent retry to return identical record ID %s, got %s", record.ID, dupRecord.ID)
	}

	publishedCount, err := svc.RelayBatch(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected relay error: %v", err)
	}

	if publishedCount != 1 {
		t.Errorf("expected 1 published message, got %d", publishedCount)
	}

	if len(producer.Messages) != 1 {
		t.Fatalf("expected producer to have received 1 message, got %d", len(producer.Messages))
	}

	expectedTopic := "order"
	if producer.Messages[0].Topic != expectedTopic {
		t.Errorf("expected topic %s, got %s", expectedTopic, producer.Messages[0].Topic)
	}
	if string(producer.Messages[0].Key) != "ord_12345" {
		t.Errorf("expected partition key ord_12345, got %s", string(producer.Messages[0].Key))
	}

	secondBatch, err := svc.RelayBatch(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected second relay error: %v", err)
	}
	if secondBatch != 0 {
		t.Errorf("expected 0 pending messages on second relay, got %d", secondBatch)
	}
}

func TestOutboxRejectsFloatingPointPrecision(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	producer := producers.NewMemoryProducerAdapter()
	svc := service.NewOutboxService(repo, producer)

	corrID, _ := uuid.NewV7()
	input := contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_corp_01",
		SourceService:    "orders-service",
		ExternalID:       "ext_float_1",
		CorrelationID:    corrID.String(),
		CausationID:      "cmd_float",
		AggregateType:    "order",
		AggregateID:      "ord_float",
		AggregateVersion: 1,
		EventType:        "price_set",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{"price": 19.99}`),
	}

	_, err := svc.StageEvent(ctx, input, "")
	if err == nil {
		t.Fatalf("expected validation error for floating point number in payload, got nil")
	}
}

func TestOutboxParkedAggregateBlocksLaterVersions(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	producer := producers.NewMemoryProducerAdapter()
	svc := service.NewOutboxService(repo, producer)

	corrID, _ := uuid.NewV7()
	input1 := contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_corp_01",
		SourceService:    "orders-service",
		ExternalID:       "ext_ord_1",
		CorrelationID:    corrID.String(),
		CausationID:      "cmd_1",
		AggregateType:    "order",
		AggregateID:      "ord_999",
		AggregateVersion: 1,
		EventType:        "created.v1",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{}`),
	}
	input2 := contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_corp_01",
		SourceService:    "orders-service",
		ExternalID:       "ext_ord_2",
		CorrelationID:    corrID.String(),
		CausationID:      "cmd_2",
		AggregateType:    "order",
		AggregateID:      "ord_999",
		AggregateVersion: 2,
		EventType:        "placed.v1",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{}`),
	}

	rec1, _ := svc.StageEvent(ctx, input1, "")
	_, _ = svc.StageEvent(ctx, input2, "")

	for i := 0; i < constants.MaxWorkerRetries; i++ {
		_ = repo.MarkFailed(ctx, rec1.TenantID, rec1.ID, "simulated broker error")
	}

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}

	if len(pending) != 0 {
		t.Errorf("expected 0 pending because aggregate is parked, got %d", len(pending))
	}
}

func TestOutboxLeaseClaimAndOwnershipVerification(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	workerA := "worker-node-a"
	workerB := "worker-node-b"

	rec := contracts.OutboxRecord{
		ID:               "rec-100",
		PublicID:         "otb_01J9XXXX",
		TenantID:         "t1",
		SourceService:    "orders",
		AggregateType:    "order",
		AggregateID:      "o-1",
		AggregateVersion: 1,
		EventType:        "created",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{}`),
		Status:           contracts.StatusPending,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	_ = repo.Save(ctx, rec)

	claimed, err := repo.ClaimInFlight(ctx, workerA, 2*time.Second, 10)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 record claimed by worker A, got %d", len(claimed))
	}

	err = repo.MarkPublishedByWorker(ctx, "t1", "rec-100", workerB)
	if err == nil {
		t.Errorf("expected error when workerB attempts to confirm workerA's record, got nil")
	}

	err = repo.MarkPublishedByWorker(ctx, "t1", "rec-100", workerA)
	if err != nil {
		t.Errorf("expected workerA to successfully publish record, got %v", err)
	}
}

func TestOutboxCanaryHeartbeatEmission(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOutboxRepositoryAdapter()
	producer := producers.NewMemoryProducerAdapter()
	svc := service.NewOutboxService(repo, producer)

	canary, err := svc.EmitCanaryHeartbeat(ctx, "tenant_corp_01", "orders-service")
	if err != nil {
		t.Fatalf("failed emitting canary heartbeat: %v", err)
	}

	if canary.AggregateType != constants.PrefixCanary {
		t.Errorf("expected canary aggregate type %s, got %s", constants.PrefixCanary, canary.AggregateType)
	}
}
