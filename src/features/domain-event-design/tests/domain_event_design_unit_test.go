package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/domain/machines"
	"event-driven-platform/src/features/domain-event-design/domain/rules"
	"event-driven-platform/src/features/domain-event-design/service"
)

func TestEventTypeValidation(t *testing.T) {
	valid := "commerce.order.placed.v1"
	if err := rules.ValidateEventType(valid); err != nil {
		t.Fatalf("expected valid event type, got %v", err)
	}

	crud := "commerce.order.created.v1"
	if err := rules.ValidateEventType(crud); err == nil {
		t.Fatalf("expected CRUD verb rejection for %s", crud)
	}

	invalidFormat := "OrderPlaced"
	if err := rules.ValidateEventType(invalidFormat); err == nil {
		t.Fatalf("expected format failure for %s", invalidFormat)
	}
}

func TestUUIDValidation(t *testing.T) {
	validUUID := "0194857b-7b2a-7963-8a9d-5a8b2413e001"
	if err := rules.ValidateUUIDv7(validUUID); err != nil {
		t.Fatalf("expected valid UUID, got %v", err)
	}

	invalidUUID := "not-a-uuid"
	if err := rules.ValidateUUIDv7(invalidUUID); err == nil {
		t.Fatalf("expected invalid UUID failure")
	}
}

func TestCausalDepthValidation(t *testing.T) {
	if err := rules.ValidateCausalDepth(0); err != nil {
		t.Fatalf("depth 0 should be valid: %v", err)
	}
	if err := rules.ValidateCausalDepth(16); err != nil {
		t.Fatalf("depth 16 should be valid: %v", err)
	}
	if err := rules.ValidateCausalDepth(-1); err == nil {
		t.Fatalf("negative depth must fail")
	}
	if err := rules.ValidateCausalDepth(17); err == nil {
		t.Fatalf("depth 17 must fail")
	}
}

func TestAggregateStateMachine(t *testing.T) {
	sm := machines.NewAggregateStateMachine()
	current := contracts.AggregateRootVersion{
		AggregateType:  "commerce.order",
		AggregateID:    "ord_123",
		CurrentVersion: 1,
		IsTerminal:     false,
	}

	cmd := contracts.ReserveAggregateVersionCommand{
		AggregateType:    "commerce.order",
		AggregateID:      "ord_123",
		ExpectedVersion:  1,
		VersionIncrement: 1,
		MarkTerminal:     false,
	}

	res, err := sm.Transition(current, cmd)
	if err != nil {
		t.Fatalf("expected successful transition: %v", err)
	}
	if res.ReservedVersion != 2 {
		t.Fatalf("expected reserved version 2, got %d", res.ReservedVersion)
	}

	conflictCmd := contracts.ReserveAggregateVersionCommand{
		AggregateType:    "commerce.order",
		AggregateID:      "ord_123",
		ExpectedVersion:  99,
		VersionIncrement: 1,
	}
	_, err = sm.Transition(current, conflictCmd)
	if err == nil {
		t.Fatalf("expected OCC conflict error")
	}

	terminalCurrent := contracts.AggregateRootVersion{
		AggregateType:  "commerce.order",
		AggregateID:    "ord_123",
		CurrentVersion: 2,
		IsTerminal:     true,
	}
	_, err = sm.Transition(terminalCurrent, cmd)
	if err == nil {
		t.Fatalf("expected terminal rejection")
	}
}

type failingGroupRepo struct{ *memoryRepo }

func (failingGroupRepo) UpsertTransactionGroup(context.Context, contracts.RegisterTransactionGroupPartCommand) (contracts.RegisterTransactionGroupPartResult, error) {
	return contracts.RegisterTransactionGroupPartResult{}, errors.New("db down")
}

type memoryRepo struct {
	catalogEntries map[string]contracts.EventCatalogEntry
	versions       map[string]contracts.AggregateRootVersion
	groups         map[string]contracts.RegisterTransactionGroupPartResult
	causals        map[string]contracts.CausalChainEntry
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{
		catalogEntries: make(map[string]contracts.EventCatalogEntry),
		versions:       make(map[string]contracts.AggregateRootVersion),
		groups:         make(map[string]contracts.RegisterTransactionGroupPartResult),
		causals:        make(map[string]contracts.CausalChainEntry),
	}
}

func (m *memoryRepo) SaveCatalogEntry(_ context.Context, entry contracts.EventCatalogEntry) error {
	m.catalogEntries[entry.EventType] = entry
	return nil
}

func (m *memoryRepo) GetCatalogEntry(_ context.Context, _ string, eventType string) (contracts.EventCatalogEntry, error) {
	if entry, ok := m.catalogEntries[eventType]; ok {
		return entry, nil
	}
	return contracts.EventCatalogEntry{
		EventType:         eventType,
		OwningAggregate:   "commerce.order",
		SemanticsClass:    contracts.SemanticsClassState,
		Visibility:        contracts.EventVisibilityPrivate,
		PIIClassification: contracts.PIIClassNone,
		IsActive:          true,
	}, nil
}

func (m *memoryRepo) GetAggregateVersion(_ context.Context, _ string, aggType string, aggID string) (contracts.AggregateRootVersion, error) {
	key := aggType + ":" + aggID
	if v, ok := m.versions[key]; ok {
		return v, nil
	}
	return contracts.AggregateRootVersion{
		AggregateType:  aggType,
		AggregateID:    aggID,
		CurrentVersion: 0,
		IsTerminal:     false,
	}, nil
}

func (m *memoryRepo) ReserveAggregateVersion(_ context.Context, cmd contracts.ReserveAggregateVersionCommand) (contracts.ReserveAggregateVersionResult, error) {
	key := cmd.AggregateType + ":" + cmd.AggregateID
	current, ok := m.versions[key]
	if !ok {
		current = contracts.AggregateRootVersion{
			AggregateType:  cmd.AggregateType,
			AggregateID:    cmd.AggregateID,
			CurrentVersion: 0,
			IsTerminal:     false,
		}
	}
	if current.IsTerminal {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrAggregateTerminal
	}
	if current.CurrentVersion != cmd.ExpectedVersion {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrOccConflict
	}
	inc := cmd.VersionIncrement
	if inc <= 0 {
		inc = 1
	}
	next := current.CurrentVersion + inc
	m.versions[key] = contracts.AggregateRootVersion{
		AggregateType:  cmd.AggregateType,
		AggregateID:    cmd.AggregateID,
		CurrentVersion: next,
		IsTerminal:     cmd.MarkTerminal,
	}
	return contracts.ReserveAggregateVersionResult{
		AggregateType:   cmd.AggregateType,
		AggregateID:     cmd.AggregateID,
		PreviousVersion: current.CurrentVersion,
		ReservedVersion: next,
		IsTerminal:      cmd.MarkTerminal,
	}, nil
}

func (m *memoryRepo) UpsertTransactionGroup(_ context.Context, cmd contracts.RegisterTransactionGroupPartCommand) (contracts.RegisterTransactionGroupPartResult, error) {
	existing, ok := m.groups[cmd.TransactionGroupID]
	if !ok {
		res := contracts.RegisterTransactionGroupPartResult{
			TransactionGroupID: cmd.TransactionGroupID,
			ExpectedGroupSize:  cmd.ExpectedGroupSize,
			ReceivedCount:      1,
			IsComplete:         cmd.ExpectedGroupSize <= 1,
		}
		m.groups[cmd.TransactionGroupID] = res
		return res, nil
	}
	existing.ReceivedCount++
	if existing.ReceivedCount >= existing.ExpectedGroupSize {
		existing.IsComplete = true
	}
	m.groups[cmd.TransactionGroupID] = existing
	return existing, nil
}

func (m *memoryRepo) RecordCausalEntry(_ context.Context, entry contracts.CausalChainEntry) error {
	m.causals[entry.EventID] = entry
	return nil
}

func (m *memoryRepo) GetCausalEntry(_ context.Context, _ string, eventID string) (contracts.CausalChainEntry, error) {
	if c, ok := m.causals[eventID]; ok {
		return c, nil
	}
	return contracts.CausalChainEntry{}, nil
}

func TestDomainEventServiceBuildEnvelope(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryRepo()
	svc := service.NewDomainEventService(repo)

	cmd := contracts.BuildEnvelopeCommand{
		TenantID:           "tenant_1",
		SourceService:      "order-service",
		EventType:          "commerce.order.placed.v1",
		AggregateType:      "commerce.order",
		AggregateID:        "ord_888",
		ExpectedVersion:    0,
		VersionIncrement:   1,
		EffectiveAt:        time.Now().UTC(),
		CausationID:        "cmd_100",
		CorrelationID:      "0194857b-7b2a-7963-8a9d-5a8b2413e001",
		CausationDepth:     1,
		TransactionGroupID: "txg_123",
		GroupSize:          2,
		Payload:            map[string]any{"total": 100},
	}

	res, err := svc.BuildCanonicalEnvelope(ctx, cmd)
	if err != nil {
		t.Fatalf("expected build envelope success: %v", err)
	}

	if res.Envelope.AggregateVersion != 1 {
		t.Fatalf("expected aggregate version 1, got %d", res.Envelope.AggregateVersion)
	}
	if res.Envelope.SemanticsClass != contracts.SemanticsClassState {
		t.Fatalf("expected STATE semantics, got %s", res.Envelope.SemanticsClass)
	}
}

func TestCausalLoopVerification(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryRepo()
	svc := service.NewDomainEventService(repo)

	cmd := contracts.VerifyCausalChainCommand{
		TenantID:       "tenant_1",
		SourceService:  "order-service",
		EventID:        "evt_same",
		CausationID:    "evt_same",
		CausationDepth: 1,
		EventType:      "commerce.order.placed.v1",
		AggregateID:    "ord_1",
	}

	res, err := svc.VerifyCausalChain(ctx, cmd)
	if err == nil {
		t.Fatalf("expected loop detection error")
	}
	if !res.LoopDetected {
		t.Fatalf("expected loop_detected = true")
	}
}


func TestBuildEnvelopeSurfacesTransactionGroupFailure(t *testing.T) {
	svc := service.NewDomainEventService(failingGroupRepo{newMemoryRepo()})
	_, err := svc.BuildCanonicalEnvelope(context.Background(), contracts.BuildEnvelopeCommand{
		TenantID: "tenant_1", SourceService: "order-service", EventType: "commerce.order.placed.v1",
		AggregateType: "commerce.order", AggregateID: "ord_889", VersionIncrement: 1,
		EffectiveAt: time.Now().UTC(), CausationID: "cmd_100",
		CorrelationID: "0194857b-7b2a-7963-8a9d-5a8b2413e001", CausationDepth: 1,
		TransactionGroupID: "txg_9", GroupSize: 2, Payload: map[string]any{"total": 1},
	})
	if !errors.Is(err, service.ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
}
