package tests

import (
	"context"
	"testing"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/domain/rules"
	"event-driven-platform/src/features/retry-topics/infrastructure/adapters"
	"event-driven-platform/src/features/retry-topics/service"
)

func TestRetryTopicsEngine(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryRetryTopicsAdapter()
	svc := service.NewRetryTopicsService(repo)

	t.Run("environmental error pauses circuit and preserves attempt budget", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassEnvironmental,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionPauseCircuit {
			t.Fatalf("expected DispositionPauseCircuit, got %s", res.Disposition)
		}
	})

	t.Run("ordered stream strictly forbids retry topic and mandates held scheduling", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassMessageTransient,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionHeldScheduled {
			t.Fatalf("expected DispositionHeldScheduled on strict ordered stream, got %s", res.Disposition)
		}
	})

	t.Run("unordered stream allows topic scheduling", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingNone,
			Classification: contracts.ClassMessageTransient,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionTopicScheduled {
			t.Fatalf("expected DispositionTopicScheduled on unordered stream, got %s", res.Disposition)
		}
	})

	t.Run("permanent data failure routes directly to dead letter", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassPermanentData,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionDeadLetter {
			t.Fatalf("expected DispositionDeadLetter, got %s", res.Disposition)
		}
	})

	t.Run("exhausted attempts route to dead letter", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassMessageTransient,
			AttemptNumber:  3,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionDeadLetter {
			t.Fatalf("expected DispositionDeadLetter on exhaustion, got %s", res.Disposition)
		}
	})

	t.Run("deadline breach marks expired disposition", func(t *testing.T) {
		deadline := time.Now().UTC().Add(500 * time.Millisecond)
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassMessageTransient,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  2000,
			MaxBackoffMs:   10000,
			ValidUntil:     &deadline,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionExpired {
			t.Fatalf("expected DispositionExpired, got %s", res.Disposition)
		}
	})

	t.Run("schedule retry records entry and generates deterministic idempotency key", func(t *testing.T) {
		tenantID := "tenant-rt"
		due := time.Now().UTC().Add(-1 * time.Second)

		key1 := rules.DeriveDeterministicIdempotencyKey("handler-1", "evt-1", "process")
		key2 := rules.DeriveDeterministicIdempotencyKey("handler-1", "evt-1", "process")
		if key1 != key2 {
			t.Fatalf("idempotency keys must be deterministic: %s != %s", key1, key2)
		}

		rec, err := svc.ScheduleRetryAttempt(ctx, contracts.ScheduleRetryInput{
			TenantID:         tenantID,
			SourceService:    "order-service",
			ExternalID:       "ext-rt-1",
			ConsumerGroup:    "order-grp",
			HandlerID:        "handler-1",
			EventID:          "evt-1",
			AggregateType:    "order",
			AggregateID:      "ord-1",
			AggregateVersion: 1,
			AttemptNumber:    1,
			MaxAttempts:      3,
			BackoffMs:        1000,
			DueAt:            due,
			Classification:   contracts.ClassMessageTransient,
			ErrorMessage:     "temporary timeout",
			ErrorCode:        "ERR_TIMEOUT",
		})
		if err != nil {
			t.Fatalf("failed scheduling retry attempt: %v", err)
		}
		if rec.PublicID == "" {
			t.Fatalf("expected public ID")
		}

		dueRecords, err := svc.GetDueRetries(ctx, tenantID, 10)
		if err != nil {
			t.Fatalf("failed getting due retries: %v", err)
		}
		if len(dueRecords) != 1 {
			t.Fatalf("expected 1 due retry record, got %d", len(dueRecords))
		}

		err = svc.MarkAttemptStatus(ctx, tenantID, rec.ID, contracts.StatusScheduled, contracts.StatusRetrying)
		if err != nil {
			t.Fatalf("failed transitioning retry status: %v", err)
		}
	})

	t.Run("causal wait classification triggers bounded causal wait hold", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassCausalWait,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionCausalWait {
			t.Fatalf("expected DispositionCausalWait, got %s", res.Disposition)
		}
	})

	t.Run("reference data missing triggers longer scheduled hold", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassReferenceDataMiss,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   60000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionHeldScheduled {
			t.Fatalf("expected DispositionHeldScheduled, got %s", res.Disposition)
		}
		if res.BackoffMs < 30000 {
			t.Fatalf("expected at least 30s backoff for reference data miss, got %d", res.BackoffMs)
		}
	})

	t.Run("contention exceeding attempt 5 escalates to held scheduled retry", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassContention,
			AttemptNumber:  6,
			MaxAttempts:    10,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionHeldScheduled {
			t.Fatalf("expected DispositionHeldScheduled on hot aggregate contention escalation, got %s", res.Disposition)
		}
	})

	t.Run("ambiguous effect triggers held scheduled retry preserving idempotency key", func(t *testing.T) {
		res, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassAmbiguousEffect,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Disposition != contracts.DispositionHeldScheduled {
			t.Fatalf("expected DispositionHeldScheduled for ambiguous effect, got %s", res.Disposition)
		}
	})

	t.Run("handler bug and crash poison route immediately to dead letter", func(t *testing.T) {
		resBug, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassHandlerBug,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resBug.Disposition != contracts.DispositionDeadLetter {
			t.Fatalf("expected DispositionDeadLetter for handler bug, got %s", resBug.Disposition)
		}

		resPoison, err := svc.EvaluateRetry(ctx, contracts.EvaluateRetryInput{
			StreamOrdering: contracts.OrderingStrict,
			Classification: contracts.ClassCrashPoison,
			AttemptNumber:  1,
			MaxAttempts:    3,
			BaseBackoffMs:  1000,
			MaxBackoffMs:   10000,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resPoison.Disposition != contracts.DispositionDeadLetter {
			t.Fatalf("expected DispositionDeadLetter for crash poison, got %s", resPoison.Disposition)
		}
	})
}
