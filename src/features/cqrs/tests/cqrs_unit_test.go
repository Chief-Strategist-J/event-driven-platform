package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/features/cqrs/domain/machines"
	"event-driven-platform/src/features/cqrs/domain/rules"
)

func TestComputeRequestFingerprint(t *testing.T) {
	fp1 := rules.ComputeRequestFingerprint(`{"amount":100,"currency":"USD"}`)
	fp2 := rules.ComputeRequestFingerprint(`{"amount":100,"currency":"USD"}`)
	fp3 := rules.ComputeRequestFingerprint(`{"amount":105,"currency":"USD"}`)

	assert.Equal(t, 64, len(fp1))
	assert.Equal(t, fp1, fp2)
	assert.NotEqual(t, fp1, fp3)
}

func TestValidateExpectedVersion(t *testing.T) {
	assert.True(t, rules.ValidateExpectedVersion(5, 5))
	assert.False(t, rules.ValidateExpectedVersion(5, 4))
	assert.False(t, rules.ValidateExpectedVersion(5, 6))
}

func TestConsistencyToken_MintAndVerify(t *testing.T) {
	secret := "test_super_secret_signing_key_32"
	now := time.Now().UTC()
	token := contracts.ConsistencyToken{
		TenantID:      "tenant_corp",
		PrincipalID:   "usr_12345",
		AggregateType: "Account",
		AggregateID:   "acc_99",
		MinVersion:    7,
		ExpiresAt:     now.Add(10 * time.Minute),
		KeyVersion:    1,
	}

	rawToken, err := rules.MintConsistencyToken(token, secret)
	assert.NoError(t, err)
	assert.NotEmpty(t, rawToken)

	verified, valid := rules.VerifyConsistencyToken(rawToken, secret, now)
	assert.True(t, valid)
	assert.NotNil(t, verified)
	assert.Equal(t, int64(7), verified.MinVersion)
	assert.Equal(t, "usr_12345", verified.PrincipalID)
}

func TestConsistencyToken_TamperedSignature(t *testing.T) {
	secret1 := "test_super_secret_signing_key_32"
	secret2 := "wrong_tampered_secret_key_32_chars"
	now := time.Now().UTC()
	token := contracts.ConsistencyToken{
		TenantID:      "tenant_corp",
		PrincipalID:   "usr_12345",
		AggregateType: "Account",
		AggregateID:   "acc_99",
		MinVersion:    7,
		ExpiresAt:     now.Add(10 * time.Minute),
		KeyVersion:    1,
	}

	rawToken, err := rules.MintConsistencyToken(token, secret1)
	assert.NoError(t, err)

	_, valid := rules.VerifyConsistencyToken(rawToken, secret2, now)
	assert.False(t, valid)
}

func TestConsistencyToken_ExpiredToken(t *testing.T) {
	secret := "test_super_secret_signing_key_32"
	now := time.Now().UTC()
	token := contracts.ConsistencyToken{
		TenantID:      "tenant_corp",
		PrincipalID:   "usr_12345",
		AggregateType: "Account",
		AggregateID:   "acc_99",
		MinVersion:    7,
		ExpiresAt:     now.Add(-1 * time.Minute),
		KeyVersion:    1,
	}

	rawToken, err := rules.MintConsistencyToken(token, secret)
	assert.NoError(t, err)

	_, valid := rules.VerifyConsistencyToken(rawToken, secret, now)
	assert.False(t, valid)
}

func TestEvaluateConsistencyResolution(t *testing.T) {
	assert.Equal(t, contracts.ResolutionImmediate, rules.EvaluateConsistencyResolution(5, 5))
	assert.Equal(t, contracts.ResolutionImmediate, rules.EvaluateConsistencyResolution(6, 5))
	assert.Equal(t, contracts.ResolutionPending, rules.EvaluateConsistencyResolution(4, 5))
}

func TestCanTransitionCommand(t *testing.T) {
	assert.True(t, machines.CanTransitionCommand(contracts.CommandStatusInProgress, contracts.CommandStatusCompleted))
	assert.True(t, machines.CanTransitionCommand(contracts.CommandStatusInProgress, contracts.CommandStatusRejected))
	assert.True(t, machines.CanTransitionCommand(contracts.CommandStatusInProgress, contracts.CommandStatusConflict))
	assert.False(t, machines.CanTransitionCommand(contracts.CommandStatusCompleted, contracts.CommandStatusInProgress))
	assert.False(t, machines.CanTransitionCommand(contracts.CommandStatusConflict, contracts.CommandStatusCompleted))
}

func TestDetermineReplayOutcome(t *testing.T) {
	record := contracts.CommandRecord{
		RequestFingerprint: "hash_abc",
		Status:             contracts.CommandStatusCompleted,
	}

	status1, match1 := machines.DetermineReplayOutcome(record, "hash_abc")
	assert.True(t, match1)
	assert.Equal(t, contracts.CommandStatusCompleted, status1)

	status2, match2 := machines.DetermineReplayOutcome(record, "hash_diverged")
	assert.False(t, match2)
	assert.Equal(t, contracts.CommandStatusRejected, status2)
}

func TestNormalizeDispatchCommandInput_Validation(t *testing.T) {
	in := contracts.DispatchCommandInput{
		TenantID:        "tenant_corp",
		PrincipalID:     "usr_1",
		SourceService:   "order-api",
		IdempotencyKey:  "key_123",
		CommandName:     "CreateOrder",
		AggregateType:   "Order",
		AggregateID:     "ord_1",
		ExpectedVersion: -1,
	}

	_, err := contracts.NormalizeDispatchCommandInput(in)
	assert.ErrorIs(t, err, contracts.ErrNegativeVersion)

	in.ExpectedVersion = 0
	norm, err := contracts.NormalizeDispatchCommandInput(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, norm.CorrelationID)
	assert.Equal(t, "key_123", norm.ExternalID)
}

func TestNormalizeRegisterSigningKeyInput_ShortSecret(t *testing.T) {
	in := contracts.RegisterSigningKeyInput{
		TenantID:       "tenant_corp",
		SourceService:  "admin",
		KeyVersion:     1,
		RawSecretValue: "short",
		ExpiresAt:      time.Now().UTC().Add(24 * time.Hour),
	}

	_, err := contracts.NormalizeRegisterSigningKeyInput(in)
	assert.ErrorIs(t, err, contracts.ErrInvalidSecretKey)
}

