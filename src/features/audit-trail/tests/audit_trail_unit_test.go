package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"event-driven-platform/src/features/audit-trail/contracts"
	"event-driven-platform/src/features/audit-trail/domain/machines"
	"event-driven-platform/src/features/audit-trail/domain/rules"
)

func TestContainsForbiddenSecrets(t *testing.T) {
	assert.True(t, rules.ContainsForbiddenSecrets(`{"password": "secret_pass"}`))
	assert.True(t, rules.ContainsForbiddenSecrets(`{"Authorization": "Bearer token123"}`))
	assert.True(t, rules.ContainsForbiddenSecrets(`{"api_key": "xyz"}`))
	assert.False(t, rules.ContainsForbiddenSecrets(`{"amount": 100, "currency": "USD"}`))
}

func TestCanonicalizeJSONPayload(t *testing.T) {
	raw := `{  "b": 2,  "a": 1  }`
	canonical, err := rules.CanonicalizeJSONPayload(raw)
	assert.NoError(t, err)
	assert.NotEmpty(t, canonical)
}

func TestComputeRecordHash(t *testing.T) {
	prev := "0000000000000000000000000000000000000000000000000000000000000000"
	secret := "test-secret-at-least-32-characters-long"
	h1 := rules.ComputeRecordHash(prev, `{"order_id":"123"}`, secret)
	h2 := rules.ComputeRecordHash(prev, `{"order_id":"123"}`, secret)
	h3 := rules.ComputeRecordHash(prev, `{"order_id":"456"}`, secret)

	assert.Equal(t, 64, len(h1))
	assert.Equal(t, h1, h2)
	assert.NotEqual(t, h1, h3)
}

func TestComputeMerkleRoot(t *testing.T) {
	h1 := rules.ComputeMerkleRoot([]string{})
	assert.Equal(t, 64, len(h1))

	hashes := []string{
		"1111111111111111111111111111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222222222222222222222222222",
		"3333333333333333333333333333333333333333333333333333333333333333",
	}
	root := rules.ComputeMerkleRoot(hashes)
	assert.Equal(t, 64, len(root))
}

func TestCheckpointSignAndVerify(t *testing.T) {
	root := "merkle_root_hash_value_1234567890abcdef"
	head := "head_record_hash_value_1234567890abcdef"
	secret := "super-secure-signing-secret-at-least-32-chars"

	sig := rules.SignCheckpointAnchor(root, head, secret)
	assert.NotEmpty(t, sig)
	assert.True(t, rules.VerifyCheckpointAnchor(root, head, secret, sig))
	assert.False(t, rules.VerifyCheckpointAnchor(root, head, "wrong-secret-must-be-32-chars-long", sig))
}

func TestCanTransitionCheckpointStatus(t *testing.T) {
	assert.True(t, machines.CanTransitionCheckpointStatus("ANCHORED", "VERIFIED"))
	assert.True(t, machines.CanTransitionCheckpointStatus("ANCHORED", "TAMPERED"))
	assert.True(t, machines.CanTransitionCheckpointStatus("VERIFIED", "TAMPERED"))
	assert.False(t, machines.CanTransitionCheckpointStatus("TAMPERED", "VERIFIED"))
}

func TestClassifyVerificationOutcome(t *testing.T) {
	assert.Equal(t, contracts.StatusTampered, machines.ClassifyVerificationOutcome("head1", "head1", 5, 10))
	assert.Equal(t, contracts.StatusIntact, machines.ClassifyVerificationOutcome("head1", "head1", 10, 10))
	assert.Equal(t, contracts.StatusMismatch, machines.ClassifyVerificationOutcome("head1", "head2", 10, 10))
}

func TestNormalizeIngestAuditRecordInput(t *testing.T) {
	in := contracts.IngestAuditRecordInput{
		TenantID:      "tenant_alpha",
		SourceService: "order-service",
		ShardID:       "shard_01",
		ActorID:       "usr_123",
		ActionName:    "orders.create",
		TargetType:    "ORDER",
		TargetID:      "ord_999",
		RawPayload:    `{"amount": 50}`,
		OccurredAt:    time.Now().UTC(),
	}

	norm, err := contracts.NormalizeIngestAuditRecordInput(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, norm.CorrelationID)
	assert.Equal(t, contracts.ClassA, norm.ComplianceClass)
	assert.Equal(t, contracts.OutcomeSucceeded, norm.Outcome)
}

func TestNormalizeCreateCheckpointInput_Validation(t *testing.T) {
	in := contracts.CreateCheckpointInput{
		TenantID:      "tenant_alpha",
		SourceService: "checkpoint-cron",
		ShardID:       "shard_01",
		SigningSecret: "short",
	}

	_, err := contracts.NormalizeCreateCheckpointInput(in)
	assert.ErrorIs(t, err, contracts.ErrInvalidSigningSecret)
}
