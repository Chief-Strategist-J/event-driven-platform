package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/infrastructure/adapters"
	"event-driven-platform/src/features/encryption/service"
)

func getTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/event_platform?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping live database test: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping live database test: %v", err)
	}
	return db
}

func TestEncryption_PostgresLifecycleAndCryptoShredding(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresEncryptionRepository(db)
	svc := service.NewEncryptionService(repo, "master-secret-32-bytes-test-key!")

	tenantID := fmt.Sprintf("tenant_%d", time.Now().UnixNano())
	subjectID := fmt.Sprintf("user_%d", time.Now().UnixNano())

	kek, err := svc.ProvisionKEK(ctx, &contracts.CreateKEKInput{
		TenantID:    tenantID,
		KeyName:     "primary-dek-wrapping-key",
		KMSProvider: contracts.KMSProviderSimulated,
		KMSKeyARN:   "arn:simulated:kms:primary",
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidateKEKPublicID(kek.PublicID))

	plainPayload := json.RawMessage(`{
		"event_id": "evt_abc123",
		"tenant_id": "` + tenantID + `",
		"user_id": "` + subjectID + `",
		"email": "customer@confidential.com",
		"social_security": "999-00-1111",
		"credit_card": "4111-2222-3333-4444",
		"status": "ORDER_PLACED"
	}`)

	encOutput, err := svc.EncryptPayload(ctx, &contracts.EncryptPayloadInput{
		TenantID:        tenantID,
		SubjectID:       subjectID,
		PlaintextJSON:   plainPayload,
		SensitiveFields: []string{"email", "social_security", "credit_card"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, encOutput.DEKPublicID)
	assert.Contains(t, encOutput.Headers, "X-Encrypted-Fields")

	var encMap map[string]interface{}
	require.NoError(t, json.Unmarshal(encOutput.EncryptedJSON, &encMap))
	assert.Equal(t, "ORDER_PLACED", encMap["status"])
	assert.NotEqual(t, "customer@confidential.com", encMap["email"])
	assert.NotEqual(t, "999-00-1111", encMap["social_security"])

	decOutput, err := svc.DecryptPayload(ctx, &contracts.DecryptPayloadInput{
		TenantID:       tenantID,
		SubjectID:      subjectID,
		EncryptedJSON:  encOutput.EncryptedJSON,
		Headers:        encOutput.Headers,
		AuthorizedRole: "accounting_consumer",
	})
	require.NoError(t, err)
	assert.False(t, decOutput.IsShredded)

	var decMap map[string]interface{}
	require.NoError(t, json.Unmarshal(decOutput.DecryptedJSON, &decMap))
	assert.Equal(t, "customer@confidential.com", decMap["email"])
	assert.Equal(t, "999-00-1111", decMap["social_security"])
	assert.Equal(t, "4111-2222-3333-4444", decMap["credit_card"])

	shredAudit, err := svc.CryptoShredSubject(ctx, &contracts.ShredSubjectInput{
		TenantID:    tenantID,
		SubjectID:   subjectID,
		Reason:      contracts.ReasonGDPRRtbf,
		RequestedBy: "dpo_privacy_officer@compliance.org",
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidateCSAPublicID(shredAudit.PublicID))

	shreddedDecOutput, err := svc.DecryptPayload(ctx, &contracts.DecryptPayloadInput{
		TenantID:       tenantID,
		SubjectID:      subjectID,
		EncryptedJSON:  encOutput.EncryptedJSON,
		Headers:        encOutput.Headers,
		AuthorizedRole: "accounting_consumer",
	})
	require.NoError(t, err)
	assert.True(t, shreddedDecOutput.IsShredded)
	assert.Equal(t, "GDPR_RTBF", shreddedDecOutput.ShreddedReason)

	var shreddedMap map[string]interface{}
	require.NoError(t, json.Unmarshal(shreddedDecOutput.DecryptedJSON, &shreddedMap))
	assert.Equal(t, "ORDER_PLACED", shreddedMap["status"])
	assert.Nil(t, shreddedMap["email"])
	assert.Nil(t, shreddedMap["social_security"])
	assert.Nil(t, shreddedMap["credit_card"])
}

func TestEncryption_PhysicalCheckConstraints(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	ctx := context.Background()

	t.Run("RejectMalformedKEKPublicID", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO encryption_kek_ledger (
				id, public_id, tenant_id, key_name, key_version, kms_provider, kms_key_arn, state
			) VALUES (
				gen_random_uuid(), 'bad_id', 't1', 'k1', 1, 'LOCAL_SIMULATED', 'arn:123', 'ACTIVE'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_encryption_kek_public_id_format")
	})

	t.Run("RejectInvalidKMSProvider", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO encryption_kek_ledger (
				id, public_id, tenant_id, key_name, key_version, kms_provider, kms_key_arn, state
			) VALUES (
				gen_random_uuid(), 'kek_1234567890abcdef', 't1', 'k1', 1, 'INVALID_KMS', 'arn:123', 'ACTIVE'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_encryption_kek_kms_provider")
	})

	t.Run("RejectInvalidDEKAlgorithm", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO encryption_dek_ledger (
				id, public_id, tenant_id, subject_id, kek_public_id, encrypted_dek_blob, algorithm, state
			) VALUES (
				gen_random_uuid(), 'dek_1234567890abcdef', 't1', 's1', 'kek_1234567890abcdef', 'blob', 'DES-56', 'ACTIVE'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_encryption_dek_algorithm")
	})

	t.Run("RejectInvalidShreddingReason", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO crypto_shredding_audit_ledger (
				id, public_id, tenant_id, subject_id, dek_public_id, reason, requested_by
			) VALUES (
				gen_random_uuid(), 'csa_1234567890abcdef', 't1', 's1', 'dek_1234567890abcdef', 'ACCIDENTAL_CLICK', 'admin'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_crypto_shredding_reason")
	})
}

func TestEncryption_TracingCompliance(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	ctx := context.Background()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	repo := adapters.NewPostgresEncryptionRepository(db)
	svc := service.NewEncryptionService(repo)

	tenantID := fmt.Sprintf("tenant_tr_%d", time.Now().UnixNano())
	subjectID := fmt.Sprintf("user_tr_%d", time.Now().UnixNano())

	_, err := svc.ProvisionKEK(ctx, &contracts.CreateKEKInput{
		TenantID:    tenantID,
		KeyName:     "primary-dek-wrapping-key",
		KMSProvider: contracts.KMSProviderSimulated,
		KMSKeyARN:   "arn:test",
	})
	require.NoError(t, err)

	_, err = svc.EncryptPayload(ctx, &contracts.EncryptPayloadInput{
		TenantID:        tenantID,
		SubjectID:       subjectID,
		PlaintextJSON:   json.RawMessage(`{"user":"bob","secret":"supersecret"}`),
		SensitiveFields: []string{"secret"},
	})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)

	var hasEncryptSpan bool
	for _, s := range spans {
		if s.Name == "EncryptionService.EncryptPayload" {
			hasEncryptSpan = true
			break
		}
	}
	assert.True(t, hasEncryptSpan)
}
