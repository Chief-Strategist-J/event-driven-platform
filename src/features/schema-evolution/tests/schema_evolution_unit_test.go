package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"event-driven-platform/src/features/schema-evolution/contracts"
	"event-driven-platform/src/features/schema-evolution/domain/machines"
	"event-driven-platform/src/features/schema-evolution/domain/rules"
)

func TestComputeSchemaFingerprint(t *testing.T) {
	fp1 := rules.ComputeSchemaFingerprint(`{"type":"record","name":"OrderCreated"}`)
	fp2 := rules.ComputeSchemaFingerprint(`{"type":"record","name":"OrderCreated"}`)
	fp3 := rules.ComputeSchemaFingerprint(`{"type":"record","name":"OrderUpdated"}`)

	assert.Equal(t, 64, len(fp1))
	assert.Equal(t, fp1, fp2)
	assert.NotEqual(t, fp1, fp3)
}

func TestEvaluateTransitiveCompatibility(t *testing.T) {
	lineage := []contracts.SchemaVersionRecord{
		{SchemaFingerprint: "fingerprint_v1"},
		{SchemaFingerprint: "fingerprint_v2"},
	}

	assert.False(t, rules.EvaluateTransitiveCompatibility("fingerprint_v1", lineage))
	assert.False(t, rules.EvaluateTransitiveCompatibility("fingerprint_v2", lineage))
	assert.True(t, rules.EvaluateTransitiveCompatibility("fingerprint_v3", lineage))
}

func TestEvaluateConsumerReadiness_AllReady(t *testing.T) {
	consumers := []contracts.ConsumerSupportRecord{
		{ConsumerGroup: "cg-orders-projection", MaxSupportedVersion: 3, Status: "ACTIVE"},
		{ConsumerGroup: "cg-notifications", MaxSupportedVersion: 4, Status: "ACTIVE"},
	}

	res := rules.EvaluateConsumerReadiness(consumers, 3)
	assert.True(t, res.IsReady)
	assert.Equal(t, 0, len(res.UnreadyConsumers))
	assert.Equal(t, 2, res.TotalActiveConsumers)
}

func TestEvaluateConsumerReadiness_UnreadyConsumer(t *testing.T) {
	consumers := []contracts.ConsumerSupportRecord{
		{ConsumerGroup: "cg-orders-projection", MaxSupportedVersion: 2, Status: "ACTIVE"},
		{ConsumerGroup: "cg-notifications", MaxSupportedVersion: 3, Status: "ACTIVE"},
		{ConsumerGroup: "cg-inactive", MaxSupportedVersion: 1, Status: "DECOMMISSIONED"},
	}

	res := rules.EvaluateConsumerReadiness(consumers, 3)
	assert.False(t, res.IsReady)
	assert.Equal(t, []string{"cg-orders-projection"}, res.UnreadyConsumers)
	assert.Equal(t, 2, res.TotalActiveConsumers)
}

func TestEvaluateDecommissionCheck_AllCriteriaMet(t *testing.T) {
	in := contracts.EvaluateDecommissionInput{
		TenantID:             "tenant_corp",
		SubjectName:          "orders-stream",
		ActiveConsumerCount:  0,
		ZeroLagDays:          10,
		TelemetryReadCount:   0,
		DecommissionAttested: true,
	}

	res := rules.EvaluateDecommissionCheck(in)
	assert.True(t, res.CanDecommission)
	assert.Empty(t, res.BlockingReasons)
}

func TestEvaluateDecommissionCheck_BlockedByLagAndConsumers(t *testing.T) {
	in := contracts.EvaluateDecommissionInput{
		TenantID:             "tenant_corp",
		SubjectName:          "orders-stream",
		ActiveConsumerCount:  1,
		ZeroLagDays:          3,
		TelemetryReadCount:   5,
		DecommissionAttested: false,
	}

	res := rules.EvaluateDecommissionCheck(in)
	assert.False(t, res.CanDecommission)
	assert.Equal(t, 4, len(res.BlockingReasons))
}

func TestClassifyDeserializationFailure(t *testing.T) {
	assert.Equal(t, contracts.IncidentSingleCorrupt, rules.ClassifyDeserializationFailure(1))
	assert.Equal(t, contracts.IncidentSingleCorrupt, rules.ClassifyDeserializationFailure(4))
	assert.Equal(t, contracts.IncidentSystemicFailure, rules.ClassifyDeserializationFailure(5))
	assert.Equal(t, contracts.IncidentSystemicFailure, rules.ClassifyDeserializationFailure(20))
}

func TestCanTransitionDualPublish(t *testing.T) {
	assert.True(t, machines.CanTransitionDualPublish("ACTIVE", "DRAINING"))
	assert.True(t, machines.CanTransitionDualPublish("ACTIVE", "EXTENDED"))
	assert.True(t, machines.CanTransitionDualPublish("DRAINING", "DECOMMISSIONED"))
	assert.False(t, machines.CanTransitionDualPublish("DECOMMISSIONED", "ACTIVE"))
}

func TestNormalizeRegisterSchemaInput_Validation(t *testing.T) {
	in := contracts.RegisterSchemaInput{
		TenantID:         "tenant_corp",
		SourceService:    "registry-cli",
		SubjectName:      "orders-created",
		SchemaVersion:    0,
		SchemaDefinition: `{"type":"record"}`,
	}

	_, err := contracts.NormalizeRegisterSchemaInput(in)
	assert.ErrorIs(t, err, contracts.ErrInvalidSchemaVersion)

	in.SchemaVersion = 1
	norm, err := contracts.NormalizeRegisterSchemaInput(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, norm.CorrelationID)
	assert.Equal(t, contracts.CompatibilityFullTransitive, norm.CompatibilityMode)
}

func TestNormalizeCheckConsumerReadinessInput_Validation(t *testing.T) {
	in := contracts.CheckConsumerReadinessInput{
		TenantID:      "tenant_corp",
		SubjectName:   "orders-created",
		TargetVersion: 0,
	}

	_, err := contracts.NormalizeCheckConsumerReadinessInput(in)
	assert.ErrorIs(t, err, contracts.ErrInvalidSchemaVersion)

	in.TargetVersion = 2
	norm, err := contracts.NormalizeCheckConsumerReadinessInput(in)
	assert.NoError(t, err)
	assert.Equal(t, 2, norm.TargetVersion)
}
