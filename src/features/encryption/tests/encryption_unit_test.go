package tests

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/domain/machines"
	"event-driven-platform/src/features/encryption/domain/rules"
)

func TestBuildAAD_Deterministic(t *testing.T) {
	ctx1 := contracts.AADContext{
		TenantID:      "tenant_a",
		SubjectID:     "sub_123",
		AggregateID:   "agg_999",
		FieldPath:     "user.contact.email",
		SchemaVersion: 1,
		KeyRef:        "dek_abc123",
	}
	ctx2 := contracts.AADContext{
		TenantID:      "tenant_a",
		SubjectID:     "sub_123",
		AggregateID:   "agg_999",
		FieldPath:     "user.contact.email",
		SchemaVersion: 1,
		KeyRef:        "dek_abc123",
	}

	assert.Equal(t, rules.BuildAAD(ctx1), rules.BuildAAD(ctx2))
}

func TestGenerateNonce_Length(t *testing.T) {
	nonce, err := rules.GenerateNonce()
	require.NoError(t, err)
	assert.Equal(t, 12, len(nonce))
}

func TestEncryptAndDecryptAESGCM_Success(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	nonce, _ := rules.GenerateNonce()
	aad := []byte("tenant_a|sub_123|agg_999|user.email|1|dek_123")
	plaintext := "confidential_patient_record"

	cHex, tHex, err := rules.EncryptAESGCM(key, nonce, []byte(plaintext), aad)
	require.NoError(t, err)
	assert.NotEmpty(t, cHex)
	assert.NotEmpty(t, tHex)

	decrypted, err := rules.DecryptAESGCM(key, nonce, cHex, tHex, aad)
	require.NoError(t, err)
	assert.Equal(t, plaintext, string(decrypted))
}

func TestDecryptAESGCM_AADMismatch_Tampered(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	nonce, _ := rules.GenerateNonce()
	aadValid := []byte("tenant_a|sub_123|agg_999|user.email|1|dek_123")
	aadTampered := []byte("tenant_b|sub_456|agg_999|user.email|1|dek_123")
	plaintext := "confidential_patient_record"

	cHex, tHex, err := rules.EncryptAESGCM(key, nonce, []byte(plaintext), aadValid)
	require.NoError(t, err)

	_, errTampered := rules.DecryptAESGCM(key, nonce, cHex, tHex, aadTampered)
	assert.ErrorIs(t, errTampered, rules.ErrTagMismatch)
}

func TestComputeBlindIndex_Deterministic(t *testing.T) {
	secret := []byte("blind_index_secret_key_32bytes_")
	h1 := rules.ComputeBlindIndex("tenant_a", "email", "John.Doe@example.com ", secret)
	h2 := rules.ComputeBlindIndex("tenant_a", "email", "john.doe@example.com", secret)
	h3 := rules.ComputeBlindIndex("tenant_a", "email", "alice@example.com", secret)

	assert.True(t, len(h1) > 10)
	assert.Equal(t, h1, h2)
	assert.NotEqual(t, h1, h3)
}

func TestCanTransitionDEKState(t *testing.T) {
	assert.True(t, machines.CanTransitionDEKState("ACTIVE", "ROTATED"))
	assert.True(t, machines.CanTransitionDEKState("ACTIVE", "SHREDDED"))
	assert.True(t, machines.CanTransitionDEKState("ROTATED", "SHREDDED"))
	assert.False(t, machines.CanTransitionDEKState("SHREDDED", "ACTIVE"))
}

func TestEvaluateDecryptOutcome_States(t *testing.T) {
	assert.Equal(t, contracts.OutcomeUnauthorized, machines.EvaluateDecryptOutcome("ACTIVE", false, nil))
	assert.Equal(t, contracts.OutcomeErased, machines.EvaluateDecryptOutcome("SHREDDED", true, nil))
	assert.Equal(t, contracts.OutcomeTampered, machines.EvaluateDecryptOutcome("ACTIVE", true, rules.ErrTagMismatch))
	assert.Equal(t, contracts.OutcomeMalformed, machines.EvaluateDecryptOutcome("ACTIVE", true, errors.New("hex error")))
	assert.Equal(t, contracts.OutcomeDecrypted, machines.EvaluateDecryptOutcome("ACTIVE", true, nil))
}

func TestNormalizeEncryptFieldInput_Validation(t *testing.T) {
	in := contracts.EncryptFieldInput{
		TenantID:    "tenant_a",
		SubjectID:   "sub_1",
		AggregateID: "agg_1",
		FieldPath:   "contact.phone",
		Plaintext:   "+1-555-0199",
	}

	norm, err := contracts.NormalizeEncryptFieldInput(in)
	require.NoError(t, err)
	assert.Equal(t, 1, norm.SchemaVersion)

	in.Plaintext = ""
	_, errEmpty := contracts.NormalizeEncryptFieldInput(in)
	assert.ErrorIs(t, errEmpty, contracts.ErrInvalidPlaintext)
}

func TestNormalizeShredSubjectInput_Validation(t *testing.T) {
	in := contracts.ShredSubjectInput{
		TenantID:    "tenant_a",
		SubjectID:   "sub_1",
		Reason:      "GDPR_RTBF",
		RequestedBy: "dpo@company.org",
	}

	norm, err := contracts.NormalizeShredSubjectInput(in)
	require.NoError(t, err)
	assert.Equal(t, "GDPR_RTBF", norm.Reason)

	in.Reason = "INVALID_REASON"
	_, errInvalid := contracts.NormalizeShredSubjectInput(in)
	assert.ErrorIs(t, errInvalid, contracts.ErrInvalidReason)
}
