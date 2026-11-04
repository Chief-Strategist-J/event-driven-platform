package tests

import (
	"context"
	"testing"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/adapters"
	"event-driven-platform/src/features/idempotent-consumers/service"
)

func TestIdempotentConsumer_HappyPathDeltaSequence(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	event1 := contracts.InboundConsumerEvent{
		EventID:          "evt-101",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-500",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-500","amount":100}`),
	}

	res1, err := svc.ConsumeEvent(ctx, event1)
	if err != nil {
		t.Fatalf("unexpected error on v1: %v", err)
	}
	if res1.Action != contracts.ActionApply {
		t.Errorf("expected ActionApply on v1, got %s", res1.Action)
	}

	event2 := contracts.InboundConsumerEvent{
		EventID:          "evt-102",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-500",
		AggregateVersion: 2,
		EventType:        "OrderPaid",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-500","status":"PAID"}`),
	}

	res2, err := svc.ConsumeEvent(ctx, event2)
	if err != nil {
		t.Fatalf("unexpected error on v2: %v", err)
	}
	if res2.Action != contracts.ActionApply {
		t.Errorf("expected ActionApply on v2, got %s", res2.Action)
	}
}

func TestIdempotentConsumer_DuplicateEventIDSkipsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	event := contracts.InboundConsumerEvent{
		EventID:          "evt-transport-dupe",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-501",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-501","amount":50}`),
	}

	res1, err := svc.ConsumeEvent(ctx, event)
	if err != nil || res1.Action != contracts.ActionApply {
		t.Fatalf("initial consume failed: %v", err)
	}

	res2, err := svc.ConsumeEvent(ctx, event)
	if err != nil {
		t.Fatalf("unexpected error on duplicate consume: %v", err)
	}
	if res2.Action != contracts.ActionSkip {
		t.Errorf("expected ActionSkip on duplicate event_id, got %s", res2.Action)
	}
}

func TestIdempotentConsumer_VersionGapHeld(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventV1 := contracts.InboundConsumerEvent{
		EventID:          "evt-v1",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-502",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-502"}`),
	}
	_, _ = svc.ConsumeEvent(ctx, eventV1)

	eventV3 := contracts.InboundConsumerEvent{
		EventID:          "evt-v3",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-502",
		AggregateVersion: 3,
		EventType:        "OrderShipped",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-502"}`),
	}

	resV3, err := svc.ConsumeEvent(ctx, eventV3)
	if err != nil {
		t.Fatalf("unexpected error on gap event: %v", err)
	}
	if resV3.Action != contracts.ActionHoldGap {
		t.Errorf("expected ActionHoldGap when version jumps from 1 to 3, got %s", resV3.Action)
	}
}

func TestIdempotentConsumer_StateSemanticsAllowsVersionJump(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventV1 := contracts.InboundConsumerEvent{
		EventID:          "evt-state-1",
		TenantID:         "tenant-corp-01",
		HandlerID:        "inventory-snapshot-handler",
		AggregateType:    "inventory",
		AggregateID:      "sku-999",
		AggregateVersion: 1,
		EventType:        "InventoryUpdated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticState,
		Payload:          []byte(`{"sku":"sku-999","available":100}`),
	}
	_, _ = svc.ConsumeEvent(ctx, eventV1)

	eventV5 := contracts.InboundConsumerEvent{
		EventID:          "evt-state-5",
		TenantID:         "tenant-corp-01",
		HandlerID:        "inventory-snapshot-handler",
		AggregateType:    "inventory",
		AggregateID:      "sku-999",
		AggregateVersion: 5,
		EventType:        "InventoryUpdated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticState,
		Payload:          []byte(`{"sku":"sku-999","available":75}`),
	}

	resV5, err := svc.ConsumeEvent(ctx, eventV5)
	if err != nil {
		t.Fatalf("unexpected error on state jump: %v", err)
	}
	if resV5.Action != contracts.ActionApply {
		t.Errorf("expected ActionApply for state semantic newer version jump, got %s", resV5.Action)
	}
}

func TestIdempotentConsumer_SplitBrainSameVersionDivergentContentParks(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventA := contracts.InboundConsumerEvent{
		EventID:          "evt-a",
		TenantID:         "tenant-corp-01",
		HandlerID:        "billing-handler",
		AggregateType:    "account",
		AggregateID:      "acc-100",
		AggregateVersion: 1,
		EventType:        "AccountDebited",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"amount":100}`),
	}
	_, _ = svc.ConsumeEvent(ctx, eventA)

	eventB := contracts.InboundConsumerEvent{
		EventID:          "evt-b",
		TenantID:         "tenant-corp-01",
		HandlerID:        "billing-handler",
		AggregateType:    "account",
		AggregateID:      "acc-100",
		AggregateVersion: 1,
		EventType:        "AccountDebited",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"amount":200}`),
	}

	resB, err := svc.ConsumeEvent(ctx, eventB)
	if err != nil {
		t.Fatalf("unexpected error on split-brain: %v", err)
	}
	if resB.Action != contracts.ActionParkPoison {
		t.Errorf("expected ActionParkPoison for same version with divergent content, got %s", resB.Action)
	}
}

func TestIdempotentConsumer_OutOfOrderInitialEventHeld(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventV2 := contracts.InboundConsumerEvent{
		EventID:          "evt-unseen-v2",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-projection-handler",
		AggregateType:    "order",
		AggregateID:      "ord-first-time-seen",
		AggregateVersion: 2,
		EventType:        "OrderUpdated",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-first-time-seen","status":"SHIPPED"}`),
	}

	res, err := svc.ConsumeEvent(ctx, eventV2)
	if err != nil {
		t.Fatalf("unexpected error on initial v2: %v", err)
	}
	if res.Action != contracts.ActionHoldGap {
		t.Errorf("expected ActionHoldGap when first seen event version is > 1 on DELTA stream, got %s", res.Action)
	}
}

func TestIdempotentConsumer_DifferentHandlersProcessSameEventID(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventForHandlerA := contracts.InboundConsumerEvent{
		EventID:          "shared-evt-uuid-001",
		TenantID:         "tenant-corp-01",
		HandlerID:        "order-email-notification-handler",
		AggregateType:    "order",
		AggregateID:      "ord-multi-handler",
		AggregateVersion: 1,
		EventType:        "OrderPlaced",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-multi-handler","total":150}`),
	}

	resA, err := svc.ConsumeEvent(ctx, eventForHandlerA)
	if err != nil || resA.Action != contracts.ActionApply {
		t.Fatalf("handler A failed to process: %v", err)
	}

	eventForHandlerB := contracts.InboundConsumerEvent{
		EventID:          "shared-evt-uuid-001",
		TenantID:         "tenant-corp-01",
		HandlerID:        "inventory-reservation-handler",
		AggregateType:    "order",
		AggregateID:      "ord-multi-handler",
		AggregateVersion: 1,
		EventType:        "OrderPlaced",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"order_id":"ord-multi-handler","total":150}`),
	}

	resB, err := svc.ConsumeEvent(ctx, eventForHandlerB)
	if err != nil {
		t.Fatalf("handler B unexpected error: %v", err)
	}
	if resB.Action != contracts.ActionApply {
		t.Errorf("expected ActionApply for handler B because dedupe scope is (handler_id, event_id), got %s", resB.Action)
	}
}

func TestIdempotentConsumer_SubsequentEventsParkedOnPoisonedAggregate(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerRepositoryAdapter()
	svc := service.NewConsumerService(repo)

	eventInitial := contracts.InboundConsumerEvent{
		EventID:          "poison-test-evt-1",
		TenantID:         "tenant-corp-01",
		HandlerID:        "payment-handler",
		AggregateType:    "payment",
		AggregateID:      "pay-900",
		AggregateVersion: 1,
		EventType:        "PaymentAuthorized",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"payment_id":"pay-900","amount":50}`),
	}
	_, _ = svc.ConsumeEvent(ctx, eventInitial)

	eventCollision := contracts.InboundConsumerEvent{
		EventID:          "poison-test-evt-1-collision",
		TenantID:         "tenant-corp-01",
		HandlerID:        "payment-handler",
		AggregateType:    "payment",
		AggregateID:      "pay-900",
		AggregateVersion: 1,
		EventType:        "PaymentAuthorized",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"payment_id":"pay-900","amount":9999}`),
	}
	resCollision, _ := svc.ConsumeEvent(ctx, eventCollision)
	if resCollision.Action != contracts.ActionParkPoison {
		t.Fatalf("expected initial collision to park, got %s", resCollision.Action)
	}

	eventSubsequent := contracts.InboundConsumerEvent{
		EventID:          "poison-test-evt-2",
		TenantID:         "tenant-corp-01",
		HandlerID:        "payment-handler",
		AggregateType:    "payment",
		AggregateID:      "pay-900",
		AggregateVersion: 2,
		EventType:        "PaymentCaptured",
		SchemaVersion:    "1.0.0",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{"payment_id":"pay-900","captured":true}`),
	}

	resSubsequent, err := svc.ConsumeEvent(ctx, eventSubsequent)
	if err != nil {
		t.Fatalf("unexpected error on subsequent event: %v", err)
	}
	if resSubsequent.Action != contracts.ActionParkPoison {
		t.Errorf("expected ActionParkPoison on subsequent version for already parked aggregate, got %s", resSubsequent.Action)
	}
}
