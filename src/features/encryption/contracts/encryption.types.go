package contracts

import (
	"time"
)

type DecryptOutcome string

const (
	OutcomeDecrypted           DecryptOutcome = "DECRYPTED"
	OutcomeErased              DecryptOutcome = "ERASED"
	OutcomeUnauthorized        DecryptOutcome = "UNAUTHORIZED"
	OutcomeUnavailable         DecryptOutcome = "UNAVAILABLE"
	OutcomeTampered            DecryptOutcome = "TAMPERED"
	OutcomeMalformed           DecryptOutcome = "MALFORMED"
	OutcomePlaintextUnexpected DecryptOutcome = "PLAINTEXT-UNEXPECTED"

	KMSProviderSimulated = "LOCAL_SIMULATED"
	KMSProviderAWS       = "AWS_KMS"
	KMSProviderGCP       = "GCP_KMS"
	KMSProviderVault     = "VAULT"

	ReasonGDPRRtbf        = "GDPR_RTBF"
	ReasonCCPADeletion    = "CCPA_DELETION"
	ReasonTenantOffboard  = "TENANT_OFFBOARDING"
	ReasonSecurityBreach  = "SECURITY_BREACH"
)

type CreateKEKInput struct {
	TenantID    string `json:"tenant_id"`
	KeyName     string `json:"key_name"`
	KMSProvider string `json:"kms_provider"`
	KMSKeyARN   string `json:"kms_key_arn"`
}

type EncryptPayloadInput struct {
	TenantID        string   `json:"tenant_id"`
	SubjectID       string   `json:"subject_id"`
	PlaintextJSON   []byte   `json:"plaintext_json"`
	SensitiveFields []string `json:"sensitive_fields"`
}

type EncryptPayloadOutput struct {
	DEKPublicID   string            `json:"dek_public_id"`
	EncryptedJSON []byte            `json:"encrypted_json"`
	Headers       map[string]string `json:"headers"`
}

type DecryptPayloadInput struct {
	TenantID       string            `json:"tenant_id"`
	SubjectID      string            `json:"subject_id"`
	EncryptedJSON  []byte            `json:"encrypted_json"`
	Headers        map[string]string `json:"headers"`
	AuthorizedRole string            `json:"authorized_role"`
}

type DecryptPayloadOutput struct {
	DecryptedJSON  []byte `json:"decrypted_json"`
	IsShredded     bool   `json:"is_shredded"`
	ShreddedReason string `json:"shredded_reason"`
}

type KEKRecord struct {
	ID          string    `json:"id"`
	PublicID    string    `json:"public_id"`
	TenantID    string    `json:"tenant_id"`
	KeyName     string    `json:"key_name"`
	KeyVersion  int       `json:"key_version"`
	KMSProvider string    `json:"kms_provider"`
	KMSKeyARN   string    `json:"kms_key_arn"`
	State       string    `json:"state"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DEKRecord struct {
	ID               string    `json:"id"`
	PublicID         string    `json:"public_id"`
	TenantID         string    `json:"tenant_id"`
	SubjectID        string    `json:"subject_id"`
	KEKPublicID      string    `json:"kek_public_id"`
	EncryptedDEKBlob string    `json:"encrypted_dek_blob"`
	Algorithm        string    `json:"algorithm"`
	State            string    `json:"state"`
	CreatedAt        time.Time `json:"created_at"`
	RotatedAt        time.Time `json:"rotated_at"`
	ShreddedAt       time.Time `json:"shredded_at"`
}

type CryptoShreddingRecord struct {
	ID          string    `json:"id"`
	PublicID    string    `json:"public_id"`
	TenantID    string    `json:"tenant_id"`
	SubjectID   string    `json:"subject_id"`
	DEKPublicID string    `json:"dek_public_id"`
	Reason      string    `json:"reason"`
	RequestedBy string    `json:"requested_by"`
	ShreddedAt  time.Time `json:"shredded_at"`
}

type EncryptedValue struct {
	SchemeVersion string `json:"scheme_version"`
	KeyRef        string `json:"key_ref"`
	Nonce         string `json:"nonce"`
	Ciphertext    string `json:"ciphertext"`
	Tag           string `json:"tag"`
}

type AADContext struct {
	TenantID      string `json:"tenant_id"`
	SubjectID     string `json:"subject_id"`
	AggregateID   string `json:"aggregate_id"`
	FieldPath     string `json:"field_path"`
	SchemaVersion int    `json:"schema_version"`
	KeyRef        string `json:"key_ref"`
}

type EncryptFieldInput struct {
	TenantID      string `json:"tenant_id"`
	SubjectID     string `json:"subject_id"`
	AggregateID   string `json:"aggregate_id"`
	FieldPath     string `json:"field_path"`
	SchemaVersion int    `json:"schema_version"`
	Plaintext     string `json:"plaintext"`
}

type DecryptFieldInput struct {
	TenantID       string         `json:"tenant_id"`
	SubjectID      string         `json:"subject_id"`
	AggregateID    string         `json:"aggregate_id"`
	FieldPath      string         `json:"field_path"`
	SchemaVersion  int            `json:"schema_version"`
	ConsumerRole   string         `json:"consumer_role"`
	EncryptedValue EncryptedValue `json:"encrypted_value"`
}

type DecryptFieldOutput struct {
	Outcome   DecryptOutcome `json:"outcome"`
	Plaintext string         `json:"plaintext,omitempty"`
	Message   string         `json:"message"`
}

type ShredSubjectInput struct {
	TenantID    string `json:"tenant_id"`
	SubjectID   string `json:"subject_id"`
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
}

type BlindIndexInput struct {
	TenantID   string `json:"tenant_id"`
	FieldClass string `json:"field_class"`
	RawValue   string `json:"raw_value"`
}

type BlindIndexOutput struct {
	BlindIndexHash string `json:"blind_index_hash"`
}
