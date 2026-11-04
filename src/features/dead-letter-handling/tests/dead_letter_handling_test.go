package tests

import (
	"context"
	"testing"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/adapters"
	"event-driven-platform/src/features/dead-letter-handling/service"
)

func TestDeadLetterHandling_QuarantineAndKeyParking(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	input := contracts.QuarantineEventInput{
		TenantID:         "tenant-corp-01",
		SourceService:    "order-service",
		ExternalID:       "ord-ext-999",
		CorrelationID:    "corr-999",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     1250,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "evt-corrupt-001",
		AggregateType:    "order",
		AggregateID:      "ord-999",
		AggregateVersion: 3,
		EventType:        "OrderCreated",
		Payload:          []byte(`{"order_id":"ord-999","invalid_field":true}`),
		FailureReason:    "schema validation constraint violation",
		ErrorCode:        "ERR_SCHEMA_MISMATCH",
	}

	rec, err := svc.QuarantineEvent(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error quarantining event: %v", err)
	}

	if rec.Status != contracts.StatusQuarantined {
		t.Errorf("expected status QUARANTINED, got %s", rec.Status)
	}

	isParked, err := svc.IsKeyParked(ctx, "tenant-corp-01", "billing-handler", "order", "ord-999")
	if err != nil || !isParked {
		t.Fatalf("expected aggregate ord-999 to be parked under DL-02, got %v", isParked)
	}
}

func TestDeadLetterHandling_SystemOutageRejectedFromQuarantine(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	input := contracts.QuarantineEventInput{
		TenantID:         "tenant-corp-01",
		SourceService:    "order-service",
		ExternalID:       "ord-ext-100",
		CorrelationID:    "corr-100",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     10,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "evt-timeout-001",
		AggregateType:    "order",
		AggregateID:      "ord-100",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		Payload:          []byte(`{"order_id":"ord-100"}`),
		FailureReason:    "database connection refused",
		ErrorCode:        "TIMEOUT",
	}

	_, err := svc.QuarantineEvent(ctx, input)
	if err == nil {
		t.Fatal("expected system timeout to be rejected under invariant DL-08, but succeeded")
	}
}

func TestDeadLetterHandling_BoundedRedriveExecution(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	input := contracts.QuarantineEventInput{
		TenantID:         "tenant-corp-01",
		SourceService:    "order-service",
		ExternalID:       "ord-ext-555",
		CorrelationID:    "corr-555",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     55,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "evt-555",
		AggregateType:    "order",
		AggregateID:      "ord-555",
		AggregateVersion: 2,
		EventType:        "OrderPaid",
		Payload:          []byte(`{"order_id":"ord-555"}`),
		FailureReason:    "transient parsing bug",
		ErrorCode:        "ERR_DATA_FORMAT",
	}

	rec, _ := svc.QuarantineEvent(ctx, input)

	failingInvoker := func(ctx context.Context, r contracts.DeadLetterQuarantineRecord) (bool, error) {
		return false, nil
	}

	for i := 1; i <= 3; i++ {
		res, err := svc.RedriveEvent(ctx, "tenant-corp-01", rec.ID, failingInvoker)
		if err != nil {
			t.Fatalf("unexpected redrive error on attempt %d: %v", i, err)
		}
		if res.Success {
			t.Errorf("expected redrive %d to fail", i)
		}
		if res.RedriveCount != i {
			t.Errorf("expected redrive count %d, got %d", i, res.RedriveCount)
		}
	}

	res4, _ := svc.RedriveEvent(ctx, "tenant-corp-01", rec.ID, failingInvoker)
	if res4.Success {
		t.Error("expected 4th redrive to be blocked by DL-11 bounded attempts")
	}

	isParked, _ := svc.IsKeyParked(ctx, "tenant-corp-01", "billing-handler", "order", "ord-555")
	if !isParked {
		t.Error("expected aggregate to remain parked after failed redrives")
	}

	err := svc.DiscardEvent(ctx, "tenant-corp-01", rec.ID, "business authorized abandonment")
	if err != nil {
		t.Fatalf("unexpected discard error: %v", err)
	}

	isParkedAfterDiscard, _ := svc.IsKeyParked(ctx, "tenant-corp-01", "billing-handler", "order", "ord-555")
	if isParkedAfterDiscard {
		t.Error("expected aggregate to be unparked following authorized discard disposition")
	}
}

func TestDeadLetterHandling_SuccessfulRedriveUnparksAggregate(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	input := contracts.QuarantineEventInput{
		TenantID:         "tenant-corp-01",
		SourceService:    "order-service",
		ExternalID:       "ord-ext-777",
		CorrelationID:    "corr-777",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     77,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "evt-777",
		AggregateType:    "order",
		AggregateID:      "ord-777",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		Payload:          []byte(`{"order_id":"ord-777"}`),
		FailureReason:    "handler patch pending",
		ErrorCode:        "ERR_HANDLER_BUG",
	}

	rec, _ := svc.QuarantineEvent(ctx, input)

	successfulInvoker := func(ctx context.Context, r contracts.DeadLetterQuarantineRecord) (bool, error) {
		return true, nil
	}

	res, err := svc.RedriveEvent(ctx, "tenant-corp-01", rec.ID, successfulInvoker)
	if err != nil || !res.Success {
		t.Fatalf("expected successful redrive: %v", err)
	}

	if res.Status != contracts.StatusRedrivenSuccess {
		t.Errorf("expected status REDRIVEN_SUCCESS, got %s", res.Status)
	}

	isParked, _ := svc.IsKeyParked(ctx, "tenant-corp-01", "billing-handler", "order", "ord-777")
	if isParked {
		t.Error("expected aggregate to be unparked following successful redrive")
	}
}

