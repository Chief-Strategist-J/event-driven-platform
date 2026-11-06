package tests

import (
	"testing"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
)

func TestPartitionRules_ValidateTopicTopology(t *testing.T) {
	t.Run("ValidOrderedTopologyPasses", func(t *testing.T) {
		top := contracts.TopicTopology{
			PartitionCount:        16,
			ReplicationFactor:     3,
			MinInSyncReplicas:     2,
			TopicClass:            contracts.TopicClassOrdered,
			AllowInPlaceExpansion: false,
		}
		if err := rules.ValidateTopicTopology(top); err != nil {
			t.Fatalf("expected valid topology to pass, got error: %v", err)
		}
	})

	t.Run("OrderedTopicRejectsInPlaceExpansion", func(t *testing.T) {
		top := contracts.TopicTopology{
			PartitionCount:        16,
			ReplicationFactor:     3,
			MinInSyncReplicas:     2,
			TopicClass:            contracts.TopicClassOrdered,
			AllowInPlaceExpansion: true,
		}
		if err := rules.ValidateTopicTopology(top); err == nil {
			t.Fatalf("expected in-place expansion on ordered topic to be rejected")
		}
	})

	t.Run("UnorderedTopicAllowsInPlaceExpansion", func(t *testing.T) {
		top := contracts.TopicTopology{
			PartitionCount:        32,
			ReplicationFactor:     3,
			MinInSyncReplicas:     2,
			TopicClass:            contracts.TopicClassUnordered,
			AllowInPlaceExpansion: true,
		}
		if err := rules.ValidateTopicTopology(top); err != nil {
			t.Fatalf("expected unordered topic in-place expansion to pass, got: %v", err)
		}
	})

	t.Run("ZeroPartitionCountRejected", func(t *testing.T) {
		top := contracts.TopicTopology{
			PartitionCount:    0,
			ReplicationFactor: 3,
			MinInSyncReplicas: 2,
			TopicClass:        contracts.TopicClassOrdered,
		}
		if err := rules.ValidateTopicTopology(top); err == nil {
			t.Fatalf("expected zero partition count to be rejected")
		}
	})
}

func TestPartitionRules_FormatAggregateKey(t *testing.T) {
	t.Run("ValidKeyConstructed", func(t *testing.T) {
		key, err := rules.FormatAggregateKey("tenant_alpha", "orders", "ord_12345")
		if err != nil {
			t.Fatalf("expected key formatting to succeed, got: %v", err)
		}
		expected := "tenant_alpha:orders:ord_12345"
		if key != expected {
			t.Fatalf("expected key %s, got %s", expected, key)
		}
	})

	t.Run("EmptyIdentifierRejected", func(t *testing.T) {
		if _, err := rules.FormatAggregateKey("", "orders", "ord_12345"); err == nil {
			t.Fatalf("expected empty tenant id to be rejected")
		}
	})

	t.Run("PIIPatternRejected", func(t *testing.T) {
		if _, err := rules.FormatAggregateKey("tenant_alpha", "users", "user@example.com"); err == nil {
			t.Fatalf("expected email PII pattern in key to be rejected")
		}
	})
}

func TestPartitionRules_CalculateSkewMetrics(t *testing.T) {
	t.Run("BalancedPartitionsStayHealthy", func(t *testing.T) {
		telemetry := []contracts.PartitionThroughput{
			{PartitionID: 0, ThroughputEPS: 100.0},
			{PartitionID: 1, ThroughputEPS: 105.0},
			{PartitionID: 2, ThroughputEPS: 95.0},
			{PartitionID: 3, ThroughputEPS: 102.0},
		}

		out, err := rules.CalculateSkewMetrics("orders.v1", telemetry)
		if err != nil {
			t.Fatalf("expected skew calculation to pass, got: %v", err)
		}
		if out.IsAlertBreached {
			t.Fatalf("expected balanced topic to not breach alert")
		}
		if out.RemediationAction != "MAINTAIN_CURRENT_ROUTING" {
			t.Fatalf("expected MAINTAIN_CURRENT_ROUTING, got %s", out.RemediationAction)
		}
	})

	t.Run("HotPartitionBreachesAlert", func(t *testing.T) {
		telemetry := []contracts.PartitionThroughput{
			{PartitionID: 0, ThroughputEPS: 5000.0},
			{PartitionID: 1, ThroughputEPS: 100.0},
			{PartitionID: 2, ThroughputEPS: 100.0},
			{PartitionID: 3, ThroughputEPS: 100.0},
		}

		out, err := rules.CalculateSkewMetrics("orders.v1", telemetry)
		if err != nil {
			t.Fatalf("expected skew calculation to pass, got: %v", err)
		}
		if !out.IsAlertBreached {
			t.Fatalf("expected hot partition to breach alert")
		}
		if out.HotPartitionID != 0 {
			t.Fatalf("expected hot partition id 0, got %d", out.HotPartitionID)
		}
		if out.RemediationAction != "ISOLATE_HOT_AGGREGATE_TO_DEDICATED_TOPIC" {
			t.Fatalf("expected ISOLATE_HOT_AGGREGATE_TO_DEDICATED_TOPIC, got %s", out.RemediationAction)
		}
	})
}

