package tests

import (
	"context"
	"testing"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/adapters"
	"event-driven-platform/src/features/event-ordering-protection/service"
)

func TestEventOrderingProtectionEngine(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryEventOrderingAdapter()
	svc := service.NewEventOrderingService(repo)

	tenantID := "tenant-alpha"
	group := "grp-orders"
	handler := "hnd-orders"
	aggType := "order"
	aggID := "ord-9901"

	t.Run("first event exact next v1 applies", func(t *testing.T) {
		res, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-1",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      aggID,
			AggregateVersion: 1,
			EventID:          "evt-1",
			EventType:        "OrderCreated",
			Payload:          []byte(`{"total": 100}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Action != contracts.ActionApply {
			t.Fatalf("expected ActionApply, got %s", res.Action)
		}
		if res.CurrentVersion != 1 {
			t.Fatalf("expected version 1, got %d", res.CurrentVersion)
		}
	})

	t.Run("duplicate v1 with same payload skipped", func(t *testing.T) {
		res, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-1-dup",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      aggID,
			AggregateVersion: 1,
			EventID:          "evt-1-dup",
			EventType:        "OrderCreated",
			Payload:          []byte(`{"total": 100}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Action != contracts.ActionDuplicateSkip {
			t.Fatalf("expected ActionDuplicateSkip, got %s", res.Action)
		}
	})

	t.Run("duplicate v1 with mutated payload triggers integrity mismatch", func(t *testing.T) {
		res, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-1-tamper",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      aggID,
			AggregateVersion: 1,
			EventID:          "evt-1-tamper",
			EventType:        "OrderCreated",
			Payload:          []byte(`{"total": 999999}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Action != contracts.ActionIntegrityMismatch {
			t.Fatalf("expected ActionIntegrityMismatch, got %s", res.Action)
		}
		if !res.IsHeld {
			t.Fatalf("expected tracker to be held")
		}
	})

	t.Run("unheld aggregate gap detection holds future event", func(t *testing.T) {
		freshAggID := "ord-9902"
		_, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-2-1",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      freshAggID,
			AggregateVersion: 1,
			EventID:          "evt-2-1",
			EventType:        "OrderCreated",
			Payload:          []byte(`{"status": "pending"}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error on v1: %v", err)
		}

		resGap, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-2-3",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      freshAggID,
			AggregateVersion: 3,
			EventID:          "evt-2-3",
			EventType:        "OrderShipped",
			Payload:          []byte(`{"tracking": "abc"}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error on v3 gap: %v", err)
		}
		if resGap.Action != contracts.ActionGapHeld {
			t.Fatalf("expected ActionGapHeld, got %s", resGap.Action)
		}
		if !resGap.IsHeld {
			t.Fatalf("expected tracker to be held on gap")
		}

		resArrival, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-2-2",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      freshAggID,
			AggregateVersion: 2,
			EventID:          "evt-2-2",
			EventType:        "OrderPaid",
			Payload:          []byte(`{"paid": true}`),
			OwnerEpoch:       1,
		})
		if err != nil {
			t.Fatalf("unexpected error on v2 arrival: %v", err)
		}
		if resArrival.Action != contracts.ActionApply {
			t.Fatalf("expected ActionApply on v2, got %s", resArrival.Action)
		}

		drainRes, err := svc.DrainHeldEvents(ctx, contracts.DrainHeldEventsInput{
			TenantID:      tenantID,
			ConsumerGroup: group,
			HandlerID:     handler,
			AggregateType: aggType,
			AggregateID:   freshAggID,
			OwnerEpoch:    1,
		})
		if err != nil {
			t.Fatalf("unexpected error on drain: %v", err)
		}
		if drainRes.EventsDrained != 1 {
			t.Fatalf("expected 1 event drained, got %d", drainRes.EventsDrained)
		}
		if drainRes.NewVersion != 3 {
			t.Fatalf("expected final version 3, got %d", drainRes.NewVersion)
		}
		if !drainRes.IsUnheld {
			t.Fatalf("expected aggregate to be unheld after drain")
		}
	})

	t.Run("epoch fencing blocks stale worker", func(t *testing.T) {
		fencedAggID := "ord-9903"
		_, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-3-1",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      fencedAggID,
			AggregateVersion: 1,
			EventID:          "evt-3-1",
			EventType:        "OrderCreated",
			Payload:          []byte(`{}`),
			OwnerEpoch:       5,
		})
		if err != nil {
			t.Fatalf("failed setup: %v", err)
		}

		resStale, err := svc.EvaluateAndApply(ctx, contracts.IngestEventSequenceInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-3-2",
			ConsumerGroup:    group,
			HandlerID:        handler,
			AggregateType:    aggType,
			AggregateID:      fencedAggID,
			AggregateVersion: 2,
			EventID:          "evt-3-2",
			EventType:        "OrderUpdated",
			Payload:          []byte(`{}`),
			OwnerEpoch:       4,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resStale.Action != contracts.ActionFencedEpochReject {
			t.Fatalf("expected ActionFencedEpochReject, got %s", resStale.Action)
		}
	})
}
