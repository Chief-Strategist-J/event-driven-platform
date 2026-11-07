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
		CorrelationID:    "5924fcd4-cb7f-581a-89b8-f47bb149ac73",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     1250,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "20d9a65a-5cc3-5ea4-a2cf-b0ff057b6ea2",
		AggregateType:    "order",
		AggregateID:      "ord-999",
		AggregateVersion: 3,
		EventType:        "OrderCreated",
		Payload:          []byte(`{"order_id":"ord-999","invalid_field":true}`),
		FailureReason:    "schema validation constraint violation",
		ErrorCode:        "ERR_SCHEMA_MISMATCH",
		OwnerEpoch:       1,
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

func TestDeadLetterHandling_CrashLoopPoisonPrecheck(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	canRun, count, err := svc.CheckAttemptPreflight(ctx, "tenant-corp-01", "order-group", "order-handler", "evt-poison", "fp-1", "C6_POISON", 3)
	if err != nil || !canRun || count != 1 {
		t.Fatalf("expected first attempt to be allowed, got canRun=%v count=%d err=%v", canRun, count, err)
	}

	svc.CheckAttemptPreflight(ctx, "tenant-corp-01", "order-group", "order-handler", "evt-poison", "fp-1", "C6_POISON", 3)
	svc.CheckAttemptPreflight(ctx, "tenant-corp-01", "order-group", "order-handler", "evt-poison", "fp-1", "C6_POISON", 3)

	canRun4, count4, err := svc.CheckAttemptPreflight(ctx, "tenant-corp-01", "order-group", "order-handler", "evt-poison", "fp-1", "C6_POISON", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if canRun4 {
		t.Errorf("expected attempt 4 to be rejected before execution (DL-15), but allowed (count=%d)", count4)
	}
}

func TestDeadLetterHandling_ParkedEventBufferingAndSequentialDrain(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewDeadLetterService(repo)

	input := contracts.QuarantineEventInput{
		TenantID:         "tenant-corp-01",
		SourceService:    "order-service",
		ExternalID:       "ord-ext-100",
		CorrelationID:    "4d685682-06c8-52bd-a50e-b20a502838a2",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     10,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "a6227e80-02da-5e10-bdcf-1e989432cbe8",
		AggregateType:    "order",
		AggregateID:      "ord-100",
		AggregateVersion: 2,
		EventType:        "OrderPaid",
		Payload:          []byte(`{"order_id":"ord-100"}`),
		FailureReason:    "parsing bug",
		ErrorCode:        "ERR_BUG",
		OwnerEpoch:       1,
	}
	rec, _ := svc.QuarantineEvent(ctx, input)

	parkedV3 := contracts.ParkedEventRecord{
		TenantID:         "tenant-corp-01",
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		AggregateType:    "order",
		AggregateID:      "ord-100",
		AggregateVersion: 3,
		EventID:          "70ba5bbe-cd53-5da3-afa9-c7675f7cd809",
		EventType:        "OrderShipped",
		Payload:          []byte(`{"order_id":"ord-100"}`),
		SourceOffset:     11,
	}
	_ = svc.BufferParkedEvent(ctx, parkedV3)

	parkedV4 := contracts.ParkedEventRecord{
		TenantID:         "tenant-corp-01",
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		AggregateType:    "order",
		AggregateID:      "ord-100",
		AggregateVersion: 4,
		EventID:          "cab148db-10de-5926-9b9c-b68f152c9f28",
		EventType:        "OrderDelivered",
		Payload:          []byte(`{"order_id":"ord-100"}`),
		SourceOffset:     12,
	}
	_ = svc.BufferParkedEvent(ctx, parkedV4)

	var appliedVersions []int64
	drainingInvoker := func(ctx context.Context, r contracts.DeadLetterQuarantineRecord) (bool, error) {
		appliedVersions = append(appliedVersions, r.AggregateVersion)
		return true, nil
	}

	res, err := svc.RedriveEvent(ctx, "tenant-corp-01", rec.ID, drainingInvoker)
	if err != nil || !res.Success {
		t.Fatalf("redrive and drain failed: %v", err)
	}

	if res.DrainedCount != 2 {
		t.Errorf("expected 2 buffered events drained, got %d", res.DrainedCount)
	}

	if len(appliedVersions) != 3 || appliedVersions[0] != 2 || appliedVersions[1] != 3 || appliedVersions[2] != 4 {
		t.Errorf("expected strict ordered drain [2, 3, 4], got %v", appliedVersions)
	}

	isParked, _ := svc.IsKeyParked(ctx, "tenant-corp-01", "billing-handler", "order", "ord-100")
	if isParked {
		t.Error("expected aggregate key to be unparked after complete drain")
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
		CorrelationID:    "5b43b2ec-3142-5606-9262-1825e5b27000",
		Topic:            "orders-v1",
		Partition:        0,
		SourceOffset:     55,
		ConsumerGroup:    "order-billing-group",
		HandlerID:        "billing-handler",
		EventID:          "4dbda7b1-7e54-5701-94b1-c94171ec6431",
		AggregateType:    "order",
		AggregateID:      "ord-555",
		AggregateVersion: 2,
		EventType:        "OrderPaid",
		Payload:          []byte(`{"order_id":"ord-555"}`),
		FailureReason:    "transient parsing bug",
		ErrorCode:        "ERR_DATA_FORMAT",
		OwnerEpoch:       1,
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
