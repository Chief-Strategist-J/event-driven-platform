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

func compatRec(v int, def string) contracts.SchemaVersionRecord {
	return contracts.SchemaVersionRecord{SchemaVersion: v, SchemaDefinition: def}
}

func TestCheckSchemaCompatibility(t *testing.T) {
	v1 := `{"type":"record","name":"E","fields":[{"name":"id","type":"string"},{"name":"amt","type":"int"}]}`
	addRequired := `{"type":"record","name":"E","fields":[{"name":"id","type":"string"},{"name":"amt","type":"int"},{"name":"cur","type":"string"}]}`
	addDefault := `{"type":"record","name":"E","fields":[{"name":"id","type":"string"},{"name":"amt","type":"int"},{"name":"cur","type":"string","default":"USD"}]}`
	removeRequired := `{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}`
	retype := `{"type":"record","name":"E","fields":[{"name":"id","type":"string"},{"name":"amt","type":"string"}]}`
	lineage := []contracts.SchemaVersionRecord{compatRec(1, v1)}

	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, addRequired, lineage))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityForward, addRequired, lineage))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityFull, addDefault, lineage))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, removeRequired, lineage))
	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityForward, removeRequired, lineage))
	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityFull, removeRequired, lineage))
	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, retype, lineage))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityNone, addRequired, lineage))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityFullTransitive, addRequired, nil))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityFull, "syntax = \"proto3\";", lineage))

	// Transitive checks every version; non-transitive only the latest.
	chain := []contracts.SchemaVersionRecord{compatRec(1, v1), compatRec(2, addDefault)}
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, addDefault, chain))
	chain = []contracts.SchemaVersionRecord{compatRec(1, v1), compatRec(2, removeRequired)}
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, removeRequired, chain))
	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityForwardTransitive, removeRequired, []contracts.SchemaVersionRecord{compatRec(1, v1), compatRec(2, removeRequired)}))
	assert.Empty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityForward, removeRequired, []contracts.SchemaVersionRecord{compatRec(1, v1), compatRec(2, removeRequired)}))
}

func TestCheckSchemaCompatibility_JSONSchema(t *testing.T) {
	old := `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`
	added := `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"required":["a","b"]}`
	lineage := []contracts.SchemaVersionRecord{compatRec(1, old)}
	assert.NotEmpty(t, rules.CheckSchemaCompatibility(contracts.CompatibilityBackward, added, lineage))
}
