package contracts

import (
	"time"
)

type TenancyTier string

const (
	TierPool      TenancyTier = "POOL"
	TierDedicated TenancyTier = "DEDICATED"
	TierSilo      TenancyTier = "SILO"
)

type TenantLifecycleState string

const (
	LifecycleOnboarding  TenantLifecycleState = "ONBOARDING"
	LifecycleActive      TenantLifecycleState = "ACTIVE"
	LifecycleSuspended   TenantLifecycleState = "SUSPENDED"
	LifecycleOffboarding TenantLifecycleState = "OFFBOARDING"
	LifecycleOffboarded  TenantLifecycleState = "OFFBOARDED"
)

type KeyStatus string

const (
	KeyStatusActive   KeyStatus = "ACTIVE"
	KeyStatusShredded KeyStatus = "SHREDDED"
)

type IntegrityViolationClass string

const (
	ViolationHeaderPayloadMismatch IntegrityViolationClass = "INTEGRITY-TENANT-HEADER-PAYLOAD"
	ViolationKeyPayloadMismatch    IntegrityViolationClass = "INTEGRITY-TENANT-KEY-PAYLOAD"
	ViolationResidencyDisallowed   IntegrityViolationClass = "INTEGRITY-TENANT-RESIDENCY"
	ViolationTenantSuspended       IntegrityViolationClass = "INTEGRITY-TENANT-SUSPENDED"
	ViolationTenantOffboarded       IntegrityViolationClass = "INTEGRITY-TENANT-OFFBOARDED"
)

type TenantRecord struct {
	ID                  string               `json:"id"`
	PublicID            string               `json:"public_id"`
	TenantID            string               `json:"tenant_id"`
	SourceService       string               `json:"source_service"`
	ExternalID          string               `json:"external_id"`
	CorrelationID       string               `json:"correlation_id"`
	Tier                TenancyTier          `json:"tier"`
	LifecycleState      TenantLifecycleState `json:"lifecycle_state"`
	HomeRegion          string               `json:"home_region"`
	AllowedRegions      []string             `json:"allowed_regions"`
	RetentionClass      string               `json:"retention_class"`
	MaxThroughputEPS    int                  `json:"max_throughput_eps"`
	MaxConcurrencyLanes int                  `json:"max_concurrency_lanes"`
	Version             int64                `json:"version"`
	CreatedAt           time.Time            `json:"created_at"`
	UpdatedAt           time.Time            `json:"updated_at"`
}

type RegisterTenantInput struct {
	TenantID            string      `json:"tenant_id"`
	SourceService       string      `json:"source_service"`
	ExternalID          string      `json:"external_id"`
	CorrelationID       string      `json:"correlation_id"`
	Tier                TenancyTier `json:"tier"`
	HomeRegion          string      `json:"home_region"`
	AllowedRegions      []string    `json:"allowed_regions"`
	RetentionClass      string      `json:"retention_class"`
	MaxThroughputEPS    int         `json:"max_throughput_eps"`
	MaxConcurrencyLanes int         `json:"max_concurrency_lanes"`
}

type UpdateLifecycleInput struct {
	TargetState TenantLifecycleState `json:"target_state"`
	Reason      string               `json:"reason"`
}

type StoreDEKInput struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	ExternalID    string `json:"external_id"`
	CorrelationID string `json:"correlation_id"`
	SubjectID     string `json:"subject_id"`
	PeriodKey     string `json:"period_key"`
	WrappedDEK    []byte `json:"wrapped_dek"`
	KEKKeyID      string `json:"kek_key_id"`
	Algorithm     string `json:"algorithm"`
}

type DEKRecord struct {
	ID            string     `json:"id"`
	PublicID      string     `json:"public_id"`
	TenantID      string     `json:"tenant_id"`
	SourceService string     `json:"source_service"`
	ExternalID    string     `json:"external_id"`
	CorrelationID string     `json:"correlation_id"`
	SubjectID     string     `json:"subject_id"`
	PeriodKey     string     `json:"period_key"`
	WrappedDEK    []byte     `json:"wrapped_dek"`
	KEKKeyID      string     `json:"kek_key_id"`
	Algorithm     string     `json:"algorithm"`
	Status        KeyStatus  `json:"status"`
	ShreddedAt    *time.Time `json:"shredded_at,omitempty"`
	Version       int64      `json:"version"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type VerifyEnvelopeInput struct {
	HeaderTenant  string `json:"header_tenant"`
	KeyPrefix     string `json:"key_prefix"`
	PayloadTenant string `json:"payload_tenant"`
	CurrentRegion string `json:"current_region"`
}

type QuotaCheckResult struct {
	Allowed            bool   `json:"allowed"`
	Throttled          bool   `json:"throttled"`
	CurrentHeldKeys    int    `json:"current_held_keys"`
	MaxHeldKeys        int    `json:"max_held_keys"`
	QuarantineCount    int    `json:"quarantine_count"`
	MaxQuarantineLimit int    `json:"max_quarantine_limit"`
	ViolationReason    string `json:"violation_reason,omitempty"`
}

