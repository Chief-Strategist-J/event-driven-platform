package client_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/messaging/engine/client"
	"event-driven-platform/src/shared/types"
)

type OrderCreatedPayload struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

func createValidEnvelope() types.EventEnvelope[OrderCreatedPayload] {
	eventID, _ := uuid.NewV7()
	return types.EventEnvelope[OrderCreatedPayload]{
		EventID:       eventID.String(),
		Type:          "commerce.order.created.v1",
		SchemaVersion: "1.0.0",
		AggregateID:   "ord_1234567890",
		AggregateSeq:  1,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		CausationID:   "req_causation_1",
		CorrelationID: "req_correlation_1",
		TenantID:      "tenant_abc",
		TraceContext: types.TraceContext{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:  "00f067aa0ba902b7",
		},
		Payload: OrderCreatedPayload{
			OrderID:     "ord_1234567890",
			AmountCents: 4500,
			Currency:    "USD",
		},
	}
}

func TestEnvelopeValidationSuccess(t *testing.T) {
	validator := client.NewEnvelopeValidator()
	env := createValidEnvelope()

	if err := client.ValidateEnvelope(validator, env); err != nil {
		t.Fatalf("expected valid envelope, got error: %v", err)
	}
}

func TestEnvelopeValidationRejectsUUIDv4(t *testing.T) {
	validator := client.NewEnvelopeValidator()
	env := createValidEnvelope()
	env.EventID = uuid.New().String()

	if err := client.ValidateEnvelope(validator, env); err == nil {
		t.Fatal("expected failure for UUIDv4, got nil")
	}
}

func TestEnvelopeValidationRejectsInvalidTypeFormat(t *testing.T) {
	validator := client.NewEnvelopeValidator()
	env := createValidEnvelope()
	env.Type = "createOrder"

	if err := client.ValidateEnvelope(validator, env); err == nil {
		t.Fatal("expected failure for invalid type naming, got nil")
	}
}

func TestEnvelopeValidationRejectsNonPositiveSequence(t *testing.T) {
	validator := client.NewEnvelopeValidator()
	env := createValidEnvelope()
	env.AggregateSeq = 0

	if err := client.ValidateEnvelope(validator, env); err == nil {
		t.Fatal("expected failure for sequence <= 0, got nil")
	}
}

func TestEnvelopeSerializationRoundTrip(t *testing.T) {
	validator := client.NewEnvelopeValidator()
	serializer := client.NewEnvelopeSerializer[OrderCreatedPayload](validator)
	original := createValidEnvelope()

	data, err := serializer.Serialize(original)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	deserialized, err := serializer.Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if deserialized.EventID != original.EventID {
		t.Errorf("expected EventID %s, got %s", original.EventID, deserialized.EventID)
	}
	if deserialized.Payload.OrderID != original.Payload.OrderID {
		t.Errorf("expected payload order id %s, got %s", original.Payload.OrderID, deserialized.Payload.OrderID)
	}
}

