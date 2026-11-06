package contracts

import (
	"fmt"
	"strings"

	"event-driven-platform/src/shared/errors"
)

func NormalizeRegisterTenantInput(raw RegisterTenantInput) (RegisterTenantInput, error) {
	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		return RegisterTenantInput{}, errors.NewValidationError("tenant_id cannot be blank")
	}

	sourceService := strings.TrimSpace(raw.SourceService)
	if sourceService == "" {
		return RegisterTenantInput{}, errors.NewValidationError("source_service cannot be blank")
	}

	externalID := strings.TrimSpace(raw.ExternalID)
	if externalID == "" {
		return RegisterTenantInput{}, errors.NewValidationError("external_id cannot be blank")
	}

	correlationID := strings.TrimSpace(raw.CorrelationID)
	if correlationID == "" {
		return RegisterTenantInput{}, errors.NewValidationError("correlation_id cannot be blank")
	}

	tier := TenancyTier(strings.ToUpper(strings.TrimSpace(string(raw.Tier))))
	if tier != TierPool && tier != TierDedicated && tier != TierSilo {
		return RegisterTenantInput{}, errors.NewValidationError(fmt.Sprintf("invalid tenancy tier: %s", raw.Tier))
	}

	homeRegion := strings.TrimSpace(raw.HomeRegion)
	if homeRegion == "" {
		return RegisterTenantInput{}, errors.NewValidationError("home_region cannot be blank")
	}

	if len(raw.AllowedRegions) == 0 {
		return RegisterTenantInput{}, errors.NewValidationError("allowed_regions must contain at least one region")
	}
	normalizedAllowed := make([]string, 0, len(raw.AllowedRegions))
	for _, reg := range raw.AllowedRegions {
		trimmed := strings.TrimSpace(reg)
		if trimmed != "" {
			normalizedAllowed = append(normalizedAllowed, trimmed)
		}
	}
	if len(normalizedAllowed) == 0 {
		return RegisterTenantInput{}, errors.NewValidationError("allowed_regions cannot contain only empty strings")
	}

	retentionClass := strings.TrimSpace(raw.RetentionClass)
	if retentionClass == "" {
		retentionClass = "DEFAULT"
	}

	maxThroughput := raw.MaxThroughputEPS
	if maxThroughput <= 0 {
		maxThroughput = 1000
	}

	maxConcurrency := raw.MaxConcurrencyLanes
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}

	return RegisterTenantInput{
		TenantID:            tenantID,
		SourceService:       sourceService,
		ExternalID:          externalID,
		CorrelationID:       correlationID,
		Tier:                tier,
		HomeRegion:          homeRegion,
		AllowedRegions:      normalizedAllowed,
		RetentionClass:      retentionClass,
		MaxThroughputEPS:    maxThroughput,
		MaxConcurrencyLanes: maxConcurrency,
	}, nil
}

func NormalizeStoreDEKInput(raw StoreDEKInput) (StoreDEKInput, error) {
	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		return StoreDEKInput{}, errors.NewValidationError("tenant_id cannot be blank")
	}

	sourceService := strings.TrimSpace(raw.SourceService)
	if sourceService == "" {
		return StoreDEKInput{}, errors.NewValidationError("source_service cannot be blank")
	}

	externalID := strings.TrimSpace(raw.ExternalID)
	if externalID == "" {
		return StoreDEKInput{}, errors.NewValidationError("external_id cannot be blank")
	}

	correlationID := strings.TrimSpace(raw.CorrelationID)
	if correlationID == "" {
		return StoreDEKInput{}, errors.NewValidationError("correlation_id cannot be blank")
	}

	subjectID := strings.TrimSpace(raw.SubjectID)
	if subjectID == "" {
		return StoreDEKInput{}, errors.NewValidationError("subject_id cannot be blank")
	}

	periodKey := strings.TrimSpace(raw.PeriodKey)
	if periodKey == "" {
		return StoreDEKInput{}, errors.NewValidationError("period_key cannot be blank")
	}

	if len(raw.WrappedDEK) == 0 {
		return StoreDEKInput{}, errors.NewValidationError("wrapped_dek cannot be empty")
	}

	kekKeyID := strings.TrimSpace(raw.KEKKeyID)
	if kekKeyID == "" {
		return StoreDEKInput{}, errors.NewValidationError("kek_key_id cannot be blank")
	}

	algo := strings.TrimSpace(raw.Algorithm)
	if algo == "" {
		algo = "AES_256_GCM"
	}

	return StoreDEKInput{
		TenantID:      tenantID,
		SourceService: sourceService,
		ExternalID:    externalID,
		CorrelationID: correlationID,
		SubjectID:     subjectID,
		PeriodKey:     periodKey,
		WrappedDEK:    raw.WrappedDEK,
		KEKKeyID:      kekKeyID,
		Algorithm:     algo,
	}, nil
}

func NormalizeVerifyEnvelopeInput(raw VerifyEnvelopeInput) (VerifyEnvelopeInput, error) {
	hdr := strings.TrimSpace(raw.HeaderTenant)
	if hdr == "" {
		return VerifyEnvelopeInput{}, errors.NewValidationError("header_tenant cannot be blank")
	}
	key := strings.TrimSpace(raw.KeyPrefix)
	if key == "" {
		return VerifyEnvelopeInput{}, errors.NewValidationError("key_prefix cannot be blank")
	}
	payload := strings.TrimSpace(raw.PayloadTenant)
	if payload == "" {
		return VerifyEnvelopeInput{}, errors.NewValidationError("payload_tenant cannot be blank")
	}
	region := strings.TrimSpace(raw.CurrentRegion)
	if region == "" {
		return VerifyEnvelopeInput{}, errors.NewValidationError("current_region cannot be blank")
	}

	return VerifyEnvelopeInput{
		HeaderTenant:  hdr,
		KeyPrefix:     key,
		PayloadTenant: payload,
		CurrentRegion: region,
	}, nil
}
