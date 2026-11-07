package tests

import (
	"testing"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
)

func validQuarantineInput() contracts.QuarantineEventInput {
	return contracts.QuarantineEventInput{
		TenantID:         "t1",
		HandlerID:        "h1",
		EventID:          "0190f3a2-7c1e-7b52-8a55-3f6d2b1c9e10",
		CorrelationID:    "0190f3a2-7c1e-7b52-8a55-3f6d2b1c9e11",
		AggregateID:      "agg-1",
		AggregateVersion: 1,
		FailureReason:    "bad payload",
	}
}

func TestValidateQuarantineInput_UUIDs(t *testing.T) {
	if err := contracts.ValidateQuarantineInput(validQuarantineInput()); err != nil {
		t.Fatalf("expected valid input, got %v", err)
	}

	bad := validQuarantineInput()
	bad.EventID = "not-a-uuid"
	if err := contracts.ValidateQuarantineInput(bad); err == nil || err.StatusCode != 400 {
		t.Fatalf("expected 400 for non-UUID eventId, got %v", err)
	}

	bad = validQuarantineInput()
	bad.CorrelationID = "corr-1"
	if err := contracts.ValidateQuarantineInput(bad); err == nil || err.StatusCode != 400 {
		t.Fatalf("expected 400 for non-UUID correlationId, got %v", err)
	}
}
