package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/domain/machines"
	"event-driven-platform/src/features/eventual-consistency-monitoring/domain/rules"
)

func TestThreeValuedConsistency_Match(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:          "tenant_alpha",
		ViewName:          "orders_view",
		AggregateType:     "Order",
		AggregateID:       "ord_100",
		SourceExists:      true,
		ViewExists:        true,
		SourceVersion:     5,
		ViewVersion:       5,
		SourcePayloadHash: "hash_abc_123",
		ViewPayloadHash:   "hash_abc_123",
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMatch, res.Outcome)
	assert.False(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_UnknownOnShardFailure(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:           "tenant_alpha",
		ViewName:           "orders_view",
		AggregateType:      "Order",
		AggregateID:        "ord_100",
		SourceExists:       true,
		ViewExists:         true,
		SourceVersion:      5,
		ViewVersion:        4,
		TargetShardsFailed: 1,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeUnknown, res.Outcome)
	assert.Equal(t, "TARGET_QUERY_OR_SHARD_DEGRADED", res.DifferencePayload)
	assert.False(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_UnknownOnQueryError(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:           "tenant_alpha",
		ViewName:           "orders_view",
		AggregateType:      "Order",
		AggregateID:        "ord_100",
		QueryErrorOccurred: true,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeUnknown, res.Outcome)
	assert.False(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_UnknownOnReplicaLag(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:            "tenant_alpha",
		ViewName:            "orders_view",
		AggregateType:       "Order",
		AggregateID:         "ord_100",
		SourceExists:        true,
		ViewExists:          true,
		SourceVersion:       5,
		ViewVersion:         5,
		ReplicaLagMs:        12000,
		ReplicaLagThreshold: 5000,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeUnknown, res.Outcome)
	assert.Equal(t, "SOURCE_REPLICA_LAG_EXCEEDED", res.DifferencePayload)
}

func TestThreeValuedConsistency_Mismatch_Missing(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:      "tenant_alpha",
		ViewName:      "orders_view",
		AggregateType: "Order",
		AggregateID:   "ord_101",
		SourceExists:  true,
		ViewExists:    false,
		SourceVersion: 1,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftMissing, res.DriftClass)
	assert.True(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_Mismatch_Ghost(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:      "tenant_alpha",
		ViewName:      "orders_view",
		AggregateType: "Order",
		AggregateID:   "ord_102",
		SourceExists:  false,
		ViewExists:    true,
		ViewVersion:   1,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftGhost, res.DriftClass)
	assert.True(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_Mismatch_VersionLag(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:          "tenant_alpha",
		ViewName:          "orders_view",
		AggregateType:     "Order",
		AggregateID:       "ord_103",
		SourceExists:      true,
		ViewExists:        true,
		SourceVersion:     10,
		ViewVersion:       8,
		SourcePayloadHash: "hash_new",
		ViewPayloadHash:   "hash_old",
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftVersionLag, res.DriftClass)
	assert.True(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_Mismatch_AttributeMismatch(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:          "tenant_alpha",
		ViewName:          "orders_view",
		AggregateType:     "Order",
		AggregateID:       "ord_104",
		SourceExists:      true,
		ViewExists:        true,
		SourceVersion:     10,
		ViewVersion:       10,
		SourcePayloadHash: "hash_calc_a",
		ViewPayloadHash:   "hash_calc_b",
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftAttributeMismatch, res.DriftClass)
	assert.True(t, res.RequiresHeal)
}

func TestThreeValuedConsistency_ManualWrite(t *testing.T) {
	in := contracts.EvaluateRecordInput{
		TenantID:      "tenant_alpha",
		ViewName:      "orders_view",
		AggregateType: "Order",
		AggregateID:   "ord_105",
		IsManualWrite: true,
	}

	res := rules.EvaluateThreeValuedConsistency(in)
	assert.Equal(t, contracts.OutcomeMismatch, res.Outcome)
	assert.Equal(t, contracts.DriftManualWrite, res.DriftClass)
	assert.False(t, res.RequiresHeal)
}

func TestMerkleBucketHash_NoXORCancellation(t *testing.T) {
	keys1 := []string{"key_1", "key_2"}
	hashes1 := []string{"h_1", "h_2"}
	digest1 := rules.ComputeMerkleBucketHash(keys1, hashes1)

	keys2 := []string{"key_1", "key_1", "key_2"}
	hashes2 := []string{"h_1", "h_1", "h_2"}
	digest2 := rules.ComputeMerkleBucketHash(keys2, hashes2)

	assert.NotEqual(t, digest1, digest2)
}

func TestCalculateWatermarkStatus(t *testing.T) {
	assert.Equal(t, "IN_SYNC", rules.CalculateWatermarkStatus(200, 1000, 0))
	assert.Equal(t, "LAGGING", rules.CalculateWatermarkStatus(1500, 1000, 10))
	assert.Equal(t, "STALLED", rules.CalculateWatermarkStatus(6000, 1000, 50))
	assert.Equal(t, "STALLED", rules.CalculateWatermarkStatus(200, 1000, 2000))
}

func TestRepeatHealBreaker(t *testing.T) {
	assert.True(t, rules.EvaluateRepeatHeal(0, 2))
	assert.True(t, rules.EvaluateRepeatHeal(1, 2))
	assert.False(t, rules.EvaluateRepeatHeal(2, 2))
	assert.False(t, rules.EvaluateRepeatHeal(3, 2))
}

func TestIncidentStateMachine(t *testing.T) {
	s1 := machines.EvaluateIncidentLifecycle(contracts.IncidentDetected, 30*time.Second, 2*time.Minute, true, 0)
	assert.Equal(t, contracts.IncidentInTolerance, s1)

	s2 := machines.EvaluateIncidentLifecycle(contracts.IncidentInTolerance, 3*time.Minute, 2*time.Minute, true, 0)
	assert.Equal(t, contracts.IncidentConfirmed, s2)

	s3 := machines.EvaluateIncidentLifecycle(contracts.IncidentConfirmed, 5*time.Minute, 2*time.Minute, true, 2)
	assert.Equal(t, contracts.IncidentManualIntervention, s3)

	sResolved := machines.EvaluateIncidentLifecycle(contracts.IncidentInTolerance, 1*time.Minute, 2*time.Minute, false, 0)
	assert.Equal(t, contracts.IncidentResolved, sResolved)
}

func TestSchemaNormalizer_SilenceExpiry(t *testing.T) {
	in := contracts.RegisterSilenceInput{
		TenantID:      "tenant_alpha",
		SourceService: "platform_admin",
		ViewName:      "orders_view",
		OwnerID:       "alice@example.com",
		Reason:        "maintenance",
		ExpiresAt:     time.Now().UTC().Add(10 * 24 * time.Hour),
	}

	_, err := contracts.NormalizeRegisterSilenceInput(in)
	assert.ErrorIs(t, err, contracts.ErrInvalidSilenceExpiry)

	in.ExpiresAt = time.Now().UTC().Add(3 * 24 * time.Hour)
	norm, err := contracts.NormalizeRegisterSilenceInput(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, norm.CorrelationID)
}

