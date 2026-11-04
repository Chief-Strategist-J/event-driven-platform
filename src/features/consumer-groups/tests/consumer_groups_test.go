package tests

import (
	"context"
	"testing"
	"time"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/domain/rules"
	"event-driven-platform/src/features/consumer-groups/infrastructure/adapters"
	"event-driven-platform/src/features/consumer-groups/service"
)

func TestConsumerGroupsEngine(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryConsumerGroupsAdapter()
	svc := service.NewConsumerGroupsService(repo)

	tenantID := "tenant-cgm"
	groupID := "orders-fulfillment-v1"
	canonicalGroupID := "prod.orders.checkout.fulfillment.v1"
	memberID := "orders-pod-0-cgm"
	instanceID := "orders-pod-0"

	t.Run("group id convention validation", func(t *testing.T) {
		err := rules.ValidateGroupID("invalid-group-id")
		if err != rules.ErrInvalidGroupIDConvention {
			t.Fatalf("expected ErrInvalidGroupIDConvention, got: %v", err)
		}

		err = rules.ValidateGroupID(canonicalGroupID)
		if err != nil {
			t.Fatalf("expected valid canonical group ID, got: %v", err)
		}
	})

	t.Run("eager assignors are strictly banned", func(t *testing.T) {
		err := rules.VerifyAssignorStrategy("org.apache.kafka.clients.consumer.RangeAssignor")
		if err == nil {
			t.Fatalf("expected error for RangeAssignor, got nil")
		}

		err = rules.VerifyAssignorStrategy("org.apache.kafka.clients.consumer.RoundRobinAssignor")
		if err == nil {
			t.Fatalf("expected error for RoundRobinAssignor, got nil")
		}

		err = rules.VerifyAssignorStrategy(contracts.CanonicalCooperativeStickyAssignor)
		if err != nil {
			t.Fatalf("expected CooperativeStickyAssignor to be allowed, got: %v", err)
		}
	})

	t.Run("member registration with cooperative sticky strategy succeeds", func(t *testing.T) {
		rec, err := svc.RegisterMember(ctx, contracts.RegisterMemberInput{
			TenantID:           tenantID,
			SourceService:      "orders-service",
			ExternalID:         "cgm-ext-1",
			GroupID:            groupID,
			MemberID:           memberID,
			InstanceID:         &instanceID,
			ClientHost:         "10.0.1.45",
			AssignmentStrategy: contracts.CanonicalCooperativeStickyAssignor,
		})
		if err != nil {
			t.Fatalf("failed registering member: %v", err)
		}
		if rec.PublicID == "" {
			t.Fatalf("expected non-empty public ID")
		}
		if rec.Status != contracts.MemberStatusJoining {
			t.Fatalf("expected JOINING status, got %s", rec.Status)
		}
	})

	t.Run("heartbeat transitions member to stable and refreshes epoch", func(t *testing.T) {
		res, err := svc.ProcessHeartbeat(ctx, contracts.HeartbeatInput{
			TenantID:   tenantID,
			GroupID:    groupID,
			MemberID:   memberID,
			InstanceID: &instanceID,
		})
		if err != nil {
			t.Fatalf("failed processing heartbeat: %v", err)
		}
		if res.Status != contracts.MemberStatusStable {
			t.Fatalf("expected STABLE status, got %s", res.Status)
		}
		if res.MustRejoin {
			t.Fatalf("expected MustRejoin false for stable member")
		}
	})

	t.Run("cooperative partition migration calculates retained revoked and added partitions", func(t *testing.T) {
		current := []contracts.PartitionAssignment{
			{Topic: "orders.placed", Partition: 0, Epoch: 1},
			{Topic: "orders.placed", Partition: 1, Epoch: 1},
		}
		incoming := []contracts.PartitionAssignment{
			{Topic: "orders.placed", Partition: 1, Epoch: 2},
			{Topic: "orders.placed", Partition: 2, Epoch: 2},
		}

		retained, revoked, added := rules.ComputeCooperativePartitionMigration(current, incoming)
		if len(retained) != 1 || retained[0].Partition != 1 {
			t.Fatalf("expected partition 1 retained, got %+v", retained)
		}
		if len(revoked) != 1 || revoked[0].Partition != 0 {
			t.Fatalf("expected partition 0 revoked, got %+v", revoked)
		}
		if len(added) != 1 || added[0].Partition != 2 {
			t.Fatalf("expected partition 2 added, got %+v", added)
		}
	})

	t.Run("rebalance sync updates member assignment and generation epoch", func(t *testing.T) {
		assigned := []contracts.PartitionAssignment{
			{Topic: "orders.placed", Partition: 1, Epoch: 2},
			{Topic: "orders.placed", Partition: 2, Epoch: 2},
		}
		revoked := []contracts.PartitionAssignment{
			{Topic: "orders.placed", Partition: 0, Epoch: 1},
		}

		logRec, err := svc.SyncRebalance(ctx, contracts.RebalanceSyncInput{
			TenantID:           tenantID,
			SourceService:      "orders-service",
			ExternalID:         "rebalance-ext-1",
			GroupID:            groupID,
			MemberID:           memberID,
			GenerationID:       2,
			RebalanceReason:    "consumer joining",
			MemberCount:        2,
			RevokedPartitions:  revoked,
			AssignedPartitions: assigned,
			DurationMs:         450,
		})
		if err != nil {
			t.Fatalf("failed syncing rebalance: %v", err)
		}
		if logRec.PublicID == "" {
			t.Fatalf("expected public ID on rebalance log")
		}

		members, err := svc.ListMembers(ctx, tenantID, groupID)
		if err != nil {
			t.Fatalf("failed listing members: %v", err)
		}
		if len(members) != 1 {
			t.Fatalf("expected 1 member, got %d", len(members))
		}
		if members[0].GenerationID != 2 {
			t.Fatalf("expected generation 2, got %d", members[0].GenerationID)
		}
	})

	t.Run("batch watchdog detects processing approaching 80 percent of max poll interval", func(t *testing.T) {
		shouldPauseSafe := rules.VerifyBatchProcessingWatchdog(100000)
		if shouldPauseSafe {
			t.Fatalf("100s processing should not trigger watchdog pause")
		}

		shouldPauseCritical := rules.VerifyBatchProcessingWatchdog(250000)
		if !shouldPauseCritical {
			t.Fatalf("250s processing (>80 percent of 300s) must trigger watchdog pause")
		}
	})

	t.Run("member liveness marks dead after session timeout", func(t *testing.T) {
		now := time.Now().UTC()
		recentHeartbeat := now.Add(-10 * time.Second)
		statusRecent := rules.EvaluateMemberLiveness(recentHeartbeat, now)
		if statusRecent != contracts.MemberStatusStable {
			t.Fatalf("expected STABLE for 10s heartbeat, got %s", statusRecent)
		}

		expiredHeartbeat := now.Add(-50 * time.Second)
		statusExpired := rules.EvaluateMemberLiveness(expiredHeartbeat, now)
		if statusExpired != contracts.MemberStatusDead {
			t.Fatalf("expected DEAD for 50s heartbeat (>45s session timeout), got %s", statusExpired)
		}
	})

	t.Run("partition epoch fence rejects stale worker epoch", func(t *testing.T) {
		err := rules.VerifyPartitionEpochFence(1, 2)
		if err != rules.ErrStalePartitionEpoch {
			t.Fatalf("expected ErrStalePartitionEpoch for workerEpoch < currentEpoch, got: %v", err)
		}

		err = rules.VerifyPartitionEpochFence(2, 2)
		if err != nil {
			t.Fatalf("expected nil for matching epoch, got: %v", err)
		}

		err = rules.VerifyPartitionEpochFence(3, 2)
		if err != nil {
			t.Fatalf("expected nil for newer epoch, got: %v", err)
		}
	})

	t.Run("watchdog escalation evaluates correct action based on stalls", func(t *testing.T) {
		action := rules.EvaluateWatchdogEscalation(500, 1000, 5000)
		if action != contracts.WatchdogActionNormal {
			t.Fatalf("expected NORMAL, got: %s", action)
		}

		action = rules.EvaluateWatchdogEscalation(6000, 1000, 5000)
		if action != contracts.WatchdogActionInterrupt {
			t.Fatalf("expected INTERRUPT_RECORD, got: %s", action)
		}

		action = rules.EvaluateWatchdogEscalation(12000, 1000, 5000)
		if action != contracts.WatchdogActionSelfFence {
			t.Fatalf("expected SELF_FENCE_PAUSE, got: %s", action)
		}

		action = rules.EvaluateWatchdogEscalation(500, 160000, 5000)
		if action != contracts.WatchdogActionGracefulExit {
			t.Fatalf("expected GRACEFUL_EXIT, got: %s", action)
		}

		action = rules.EvaluateWatchdogEscalation(500, 190000, 5000)
		if action != contracts.WatchdogActionHardExit {
			t.Fatalf("expected HARD_EXIT, got: %s", action)
		}
	})

	t.Run("lost partition evaluation disallows commits", func(t *testing.T) {
		allowed := rules.EvaluateLostPartitionDisallowCommit(true)
		if allowed {
			t.Fatalf("expected commit not allowed for lost partition")
		}

		allowed = rules.EvaluateLostPartitionDisallowCommit(false)
		if !allowed {
			t.Fatalf("expected commit allowed for active partition")
		}
	})

	t.Run("rebalance storm detection flags excessive rebalances within window", func(t *testing.T) {
		err := rules.EvaluateRebalanceStorm(2, 5*time.Minute)
		if err != nil {
			t.Fatalf("expected no storm for 2 rebalances, got: %v", err)
		}

		err = rules.EvaluateRebalanceStorm(4, 5*time.Minute)
		if err != rules.ErrRebalanceStormDetected {
			t.Fatalf("expected ErrRebalanceStormDetected for 4 rebalances, got: %v", err)
		}
	})
}

func BenchmarkConsumerGroups_ComputeCooperativeMigration(b *testing.B) {
	current := []contracts.PartitionAssignment{
		{Topic: "orders.placed", Partition: 0, Epoch: 1},
		{Topic: "orders.placed", Partition: 1, Epoch: 1},
		{Topic: "orders.placed", Partition: 2, Epoch: 1},
		{Topic: "orders.placed", Partition: 3, Epoch: 1},
	}
	incoming := []contracts.PartitionAssignment{
		{Topic: "orders.placed", Partition: 2, Epoch: 2},
		{Topic: "orders.placed", Partition: 3, Epoch: 2},
		{Topic: "orders.placed", Partition: 4, Epoch: 2},
		{Topic: "orders.placed", Partition: 5, Epoch: 2},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		retained, revoked, added := rules.ComputeCooperativePartitionMigration(current, incoming)
		if len(retained) == 0 || len(revoked) == 0 || len(added) == 0 {
			b.Fatal("invalid computation")
		}
	}
}

func BenchmarkConsumerGroups_EvaluateWatchdogEscalation(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		action := rules.EvaluateWatchdogEscalation(6000, 1000, 5000)
		if action != contracts.WatchdogActionInterrupt {
			b.Fatal("invalid watchdog action")
		}
	}
}



