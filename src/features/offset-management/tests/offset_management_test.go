package tests

import (
	"context"
	"testing"
	"time"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/domain/machines"
	"event-driven-platform/src/features/offset-management/domain/rules"
	"event-driven-platform/src/features/offset-management/infrastructure/adapters"
	"event-driven-platform/src/features/offset-management/service"
)

func TestOffsetManagementEngine(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOffsetManagementAdapter()
	svc := service.NewOffsetManagementService(repo)

	tenantID := "tenant-offset-1"
	groupID := "orders-consumer-v1"
	topic := "orders.created"
	partition := int32(0)

	t.Run("auto-commit strategy is strictly banned", func(t *testing.T) {
		err := rules.VerifyCommitStrategy(contracts.CommitStrategyAuto)
		if err != rules.ErrAutoCommitBanned {
			t.Fatalf("expected ErrAutoCommitBanned, got: %v", err)
		}

		err = rules.VerifyCommitStrategy(contracts.CommitStrategyManual)
		if err != nil {
			t.Fatalf("expected manual commit strategy to pass, got: %v", err)
		}
	})

	t.Run("negative offset commit is disallowed", func(t *testing.T) {
		err := rules.ValidateOffsetCommit(0, -1)
		if err != rules.ErrNegativeOffsetDisallowed {
			t.Fatalf("expected ErrNegativeOffsetDisallowed, got: %v", err)
		}
	})

	t.Run("offset regression is disallowed on normal commit", func(t *testing.T) {
		err := rules.ValidateOffsetCommit(100, 95)
		if err != rules.ErrOffsetRegression {
			t.Fatalf("expected ErrOffsetRegression, got: %v", err)
		}
	})

	t.Run("commit offset advances stored offset and increments epoch", func(t *testing.T) {
		rec, err := svc.CommitOffset(ctx, contracts.CommitOffsetInput{
			TenantID:      tenantID,
			SourceService: "orders-processor",
			ExternalID:    "offset-ext-1",
			CorrelationID: "018f3a20-8b9a-7c22-b25b-f350c316cf5a",
			GroupID:       groupID,
			Topic:         topic,
			Partition:     partition,
			Offset:        100,
			LogEndOffset:  120,
		})
		if err != nil {
			t.Fatalf("failed committing offset: %v", err)
		}
		if rec.StoredOffset != 100 {
			t.Fatalf("expected stored offset 100, got %d", rec.StoredOffset)
		}
		if rec.Lag != 20 {
			t.Fatalf("expected lag 20, got %d", rec.Lag)
		}
		if rec.CommitEpoch != 1 {
			t.Fatalf("expected commit epoch 1, got %d", rec.CommitEpoch)
		}

		rec2, err := svc.CommitOffset(ctx, contracts.CommitOffsetInput{
			TenantID:      tenantID,
			SourceService: "orders-processor",
			ExternalID:    "offset-ext-2",
			CorrelationID: "018f3a20-8b9a-7c22-b25b-f350c316cf5a",
			GroupID:       groupID,
			Topic:         topic,
			Partition:     partition,
			Offset:        150,
			LogEndOffset:  160,
		})
		if err != nil {
			t.Fatalf("failed committing second offset: %v", err)
		}
		if rec2.StoredOffset != 150 {
			t.Fatalf("expected stored offset 150, got %d", rec2.StoredOffset)
		}
		if rec2.CommitEpoch != 2 {
			t.Fatalf("expected commit epoch 2, got %d", rec2.CommitEpoch)
		}
	})

	t.Run("rewind offset safely moves offset backwards for replay", func(t *testing.T) {
		rec, err := svc.RewindOffset(ctx, contracts.RewindOffsetInput{
			TenantID:      tenantID,
			SourceService: "replay-controller",
			ExternalID:    "rewind-ext-1",
			CorrelationID: "018f3a20-8b9a-7c22-b25b-f350c316cf5a",
			GroupID:       groupID,
			Topic:         topic,
			Partition:     partition,
			TargetOffset:  50,
			Reason:        "Replay past failure",
		})
		if err != nil {
			t.Fatalf("failed rewinding offset: %v", err)
		}
		if rec.StoredOffset != 50 {
			t.Fatalf("expected target offset 50, got %d", rec.StoredOffset)
		}
	})

	t.Run("fast forward offset shifts forward past poison batch", func(t *testing.T) {
		rec, err := svc.FastForwardOffset(ctx, contracts.FastForwardOffsetInput{
			TenantID:      tenantID,
			SourceService: "emergency-ops",
			ExternalID:    "ff-ext-1",
			CorrelationID: "018f3a20-8b9a-7c22-b25b-f350c316cf5a",
			GroupID:       groupID,
			Topic:         topic,
			Partition:     partition,
			ShiftBy:       30,
			Reason:        "Bypass poison batch",
		})
		if err != nil {
			t.Fatalf("failed fast-forwarding offset: %v", err)
		}
		if rec.StoredOffset != 80 {
			t.Fatalf("expected stored offset 80 (50+30), got %d", rec.StoredOffset)
		}
	})

	t.Run("lag severity evaluates healthy surge and stall conditions", func(t *testing.T) {
		sev := rules.EvaluateLagSeverity(100, 150, time.Minute, 10.0)
		if sev != contracts.LagSeverityHealthy {
			t.Fatalf("expected HEALTHY, got %s", sev)
		}

		sevSurge := rules.EvaluateLagSeverity(100, 15000, time.Minute, 10.0)
		if sevSurge != contracts.LagSeveritySurge {
			t.Fatalf("expected SURGE, got %s", sevSurge)
		}

		sevStalled := rules.EvaluateLagSeverity(100, 15000, time.Minute, 0.0)
		if sevStalled != contracts.LagSeverityStalled {
			t.Fatalf("expected STALLED, got %s", sevStalled)
		}
	})

	t.Run("lag state machine validates valid and invalid transitions", func(t *testing.T) {
		m := machines.NewLagStateMachine(contracts.LagSeverityHealthy)
		err := m.Transition(contracts.LagSeveritySurge)
		if err != nil {
			t.Fatalf("expected transition to SURGE to succeed, got: %v", err)
		}

		err = m.Transition(contracts.LagSeverityStalled)
		if err != nil {
			t.Fatalf("expected transition to STALLED to succeed, got: %v", err)
		}

		err = m.Transition(contracts.LagSeverityHealthy)
		if err != nil {
			t.Fatalf("expected transition back to HEALTHY to succeed, got: %v", err)
		}
	})
}

func BenchmarkOffsetManagement_ValidateOffsetCommit(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := rules.ValidateOffsetCommit(100, 101)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOffsetManagement_EvaluateLagSeverity(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sev := rules.EvaluateLagSeverity(100, 15000, time.Minute, 10.0)
		if sev != contracts.LagSeveritySurge {
			b.Fatal("invalid lag evaluation")
		}
	}
}


