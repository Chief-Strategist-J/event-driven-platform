package tests

import (
	"strings"
	"testing"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/domain/machines"
	"event-driven-platform/src/features/transactional-outbox/domain/rules"
)

func TestOutboxStatusTransitions(t *testing.T) {
	sm := machines.NewOutboxStateMachine()

	next, err := sm.TransitionTo(contracts.StatusPending, contracts.StatusPublished)
	if err != nil || next != contracts.StatusPublished {
		t.Fatalf("expected PENDING -> PUBLISHED to succeed, got %v", err)
	}

	next, err = sm.TransitionTo(contracts.StatusPending, contracts.StatusParked)
	if err != nil || next != contracts.StatusParked {
		t.Fatalf("expected PENDING -> PARKED to succeed, got %v", err)
	}

	next, err = sm.TransitionTo(contracts.StatusParked, contracts.StatusPending)
	if err != nil || next != contracts.StatusPending {
		t.Fatalf("expected PARKED -> PENDING to succeed, got %v", err)
	}

	_, err = sm.TransitionTo(contracts.StatusPublished, contracts.StatusPending)
	if err == nil {
		t.Fatal("expected PUBLISHED -> PENDING to fail, got nil")
	}
}

func TestValidateOutboxInputRejectsEmptyFields(t *testing.T) {
	input := contracts.CreateOutboxRecordInput{
		TenantID:         "",
		SourceService:    "",
		AggregateType:    "",
		AggregateID:      "",
		AggregateVersion: 0,
		EventType:        "",
		Payload:          nil,
	}

	err := rules.ValidateOutboxInput(input)
	if err == nil {
		t.Fatal("expected validation error for empty input, got nil")
	}

	if len(err.Details) < 5 {
		t.Errorf("expected at least 5 validation violations, got %d", len(err.Details))
	}
}

func TestValidateOutboxInputRejectsOversizedPayload(t *testing.T) {
	oversizedPayload := []byte(strings.Repeat("a", rules.MaxOutboxPayloadBytes+1))
	input := contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_test",
		SourceService:    "order-service",
		AggregateType:    "order",
		AggregateID:      "ord_1",
		AggregateVersion: 1,
		EventType:        "placed.v1",
		Payload:          oversizedPayload,
	}

	err := rules.ValidateOutboxInput(input)
	if err == nil {
		t.Fatal("expected rejection for payload > 1MB (F1.7), got nil")
	}
}

