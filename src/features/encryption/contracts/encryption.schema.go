package contracts

import (
	"errors"
	"strings"
)

var (
	ErrInvalidTenantID     = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidSubjectID    = errors.New("subject_id must not be empty or whitespace")
	ErrInvalidAggregateID  = errors.New("aggregate_id must not be empty or whitespace")
	ErrInvalidFieldPath    = errors.New("field_path must not be empty or whitespace")
	ErrInvalidPlaintext    = errors.New("plaintext must not be empty")
	ErrInvalidKeyRef       = errors.New("key_ref must not be empty")
	ErrInvalidCiphertext   = errors.New("ciphertext must not be empty")
	ErrInvalidNonce        = errors.New("nonce must not be empty")
	ErrInvalidReason       = errors.New("invalid crypto-shredding reason")
	ErrInvalidRequestedBy  = errors.New("requested_by must not be empty or whitespace")
	ErrInvalidFieldClass   = errors.New("field_class must not be empty or whitespace")
	ErrInvalidRawValue     = errors.New("raw_value must not be empty or whitespace")
)

func ValidateKEKPublicID(publicID string) bool {
	return strings.HasPrefix(publicID, "kek_") && len(publicID) >= 20
}

func ValidateCSAPublicID(publicID string) bool {
	return strings.HasPrefix(publicID, "csa_") && len(publicID) >= 20
}

func NormalizeEncryptFieldInput(in EncryptFieldInput) (EncryptFieldInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SubjectID = strings.TrimSpace(in.SubjectID)
	if in.SubjectID == "" {
		return in, ErrInvalidSubjectID
	}
	in.AggregateID = strings.TrimSpace(in.AggregateID)
	if in.AggregateID == "" {
		return in, ErrInvalidAggregateID
	}
	in.FieldPath = strings.TrimSpace(in.FieldPath)
	if in.FieldPath == "" {
		return in, ErrInvalidFieldPath
	}
	if in.Plaintext == "" {
		return in, ErrInvalidPlaintext
	}
	if in.SchemaVersion <= 0 {
		in.SchemaVersion = 1
	}
	return in, nil
}

func NormalizeDecryptFieldInput(in DecryptFieldInput) (DecryptFieldInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SubjectID = strings.TrimSpace(in.SubjectID)
	if in.SubjectID == "" {
		return in, ErrInvalidSubjectID
	}
	in.AggregateID = strings.TrimSpace(in.AggregateID)
	if in.AggregateID == "" {
		return in, ErrInvalidAggregateID
	}
	in.FieldPath = strings.TrimSpace(in.FieldPath)
	if in.FieldPath == "" {
		return in, ErrInvalidFieldPath
	}
	if in.EncryptedValue.KeyRef == "" {
		return in, ErrInvalidKeyRef
	}
	if in.EncryptedValue.Ciphertext == "" {
		return in, ErrInvalidCiphertext
	}
	if in.EncryptedValue.Nonce == "" {
		return in, ErrInvalidNonce
	}
	if in.SchemaVersion <= 0 {
		in.SchemaVersion = 1
	}
	if in.ConsumerRole == "" {
		in.ConsumerRole = "DATA_READER"
	}
	return in, nil
}

func NormalizeShredSubjectInput(in ShredSubjectInput) (ShredSubjectInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SubjectID = strings.TrimSpace(in.SubjectID)
	if in.SubjectID == "" {
		return in, ErrInvalidSubjectID
	}
	in.Reason = strings.TrimSpace(in.Reason)
	switch in.Reason {
	case "GDPR_RTBF", "CCPA_DELETION", "TENANT_OFFBOARDING", "SECURITY_BREACH":
	default:
		return in, ErrInvalidReason
	}
	in.RequestedBy = strings.TrimSpace(in.RequestedBy)
	if in.RequestedBy == "" {
		return in, ErrInvalidRequestedBy
	}
	return in, nil
}

func NormalizeBlindIndexInput(in BlindIndexInput) (BlindIndexInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.FieldClass = strings.TrimSpace(in.FieldClass)
	if in.FieldClass == "" {
		return in, ErrInvalidFieldClass
	}
	in.RawValue = strings.TrimSpace(in.RawValue)
	if in.RawValue == "" {
		return in, ErrInvalidRawValue
	}
	return in, nil
}
