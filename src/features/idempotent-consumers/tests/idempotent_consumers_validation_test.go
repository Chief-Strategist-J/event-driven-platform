package tests

import (
	"testing"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
)

func validInboundEvent() contracts.InboundConsumerEvent {
	return contracts.InboundConsumerEvent{
		EventID:          "0190f3a2-7c1e-7b52-8a55-3f6d2b1c9e10",
		TenantID:         "t1",
		HandlerID:        "h1",
		AggregateType:    "Order",
		AggregateID:      "o1",
		AggregateVersion: 1,
		EventType:        "OrderCreated",
		SemanticClass:    contracts.SemanticDelta,
		Payload:          []byte(`{}`),
	}
}

func TestValidateInboundConsumerEvent_EventIDAndSemanticClass(t *testing.T) {
	if err := contracts.ValidateInboundConsumerEvent(validInboundEvent()); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}

	ev := validInboundEvent()
	ev.EventID = "evt-1"
	if err := contracts.ValidateInboundConsumerEvent(ev); err == nil || err.StatusCode != 400 {
		t.Fatalf("expected 400 for non-UUID eventId, got %v", err)
	}

	ev = validInboundEvent()
	ev.SemanticClass = contracts.SemanticClass("BOGUS")
	if err := contracts.ValidateInboundConsumerEvent(ev); err == nil || err.StatusCode != 400 {
		t.Fatalf("expected 400 for bogus semanticClass, got %v", err)
	}
}
