package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/rules"
	"event-driven-platform/src/features/multi-tenant-event-isolation/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/utils"
)

type MultiTenantService struct {
	repo    ports.TenantRepositoryPort
	crypto  ports.CryptoKeystorePort
	machine *machines.TenantLifecycleMachine
}

func NewMultiTenantService(
	repo ports.TenantRepositoryPort,
	crypto ports.CryptoKeystorePort,
	machine *machines.TenantLifecycleMachine,
) *MultiTenantService {
	return &MultiTenantService{
		repo:    repo,
		crypto:  crypto,
		machine: machine,
	}
}

func (s *MultiTenantService) RegisterTenant(ctx context.Context, raw contracts.RegisterTenantInput) (*contracts.TenantRecord, error) {
	norm, err := contracts.NormalizeRegisterTenantInput(raw)
	if err != nil {
		return nil, err
	}

	existing, _ := s.repo.GetTenant(ctx, norm.TenantID)
	if existing != nil {
		return nil, errors.NewConflictError(fmt.Sprintf("tenant already registered: %s", norm.TenantID))
	}

	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("tnt_%s", strings.ReplaceAll(id, "-", "")[:20])
	now := time.Now().UTC()

	rec := contracts.TenantRecord{
		ID:                  id,
		PublicID:            publicID,
		TenantID:            norm.TenantID,
		SourceService:       norm.SourceService,
		ExternalID:          norm.ExternalID,
		CorrelationID:       norm.CorrelationID,
		Tier:                norm.Tier,
		LifecycleState:      contracts.LifecycleActive,
		HomeRegion:          norm.HomeRegion,
		AllowedRegions:      norm.AllowedRegions,
		RetentionClass:      norm.RetentionClass,
		MaxThroughputEPS:    norm.MaxThroughputEPS,
		MaxConcurrencyLanes: norm.MaxConcurrencyLanes,
		Version:             1,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if insertErr := s.repo.RegisterTenant(ctx, rec); insertErr != nil {
		return nil, insertErr
	}

	_ = s.repo.AppendAuditLog(ctx, norm.TenantID, "TENANT_REGISTERED", norm.SourceService, norm.ExternalID, norm.CorrelationID, map[string]interface{}{
		"tier":        norm.Tier,
		"home_region": norm.HomeRegion,
	})

	return &rec, nil
}

func (s *MultiTenantService) GetTenant(ctx context.Context, tenantID string) (*contracts.TenantRecord, error) {
	trimmed := strings.TrimSpace(tenantID)
	if trimmed == "" {
		return nil, errors.NewValidationError("tenant_id cannot be blank")
	}
	return s.repo.GetTenant(ctx, trimmed)
}

func (s *MultiTenantService) TransitionLifecycle(ctx context.Context, tenantID string, raw contracts.UpdateLifecycleInput) (*contracts.TenantRecord, error) {
	trimmed := strings.TrimSpace(tenantID)
	tenant, err := s.repo.GetTenant(ctx, trimmed)
	if err != nil {
		return nil, err
	}

	targetState := contracts.TenantLifecycleState(strings.ToUpper(strings.TrimSpace(string(raw.TargetState))))
	nextState, transErr := s.machine.Transition(tenant.LifecycleState, targetState)
	if transErr != nil {
		return nil, transErr
	}

	if updateErr := s.repo.UpdateLifecycleState(ctx, trimmed, nextState, tenant.Version); updateErr != nil {
		return nil, updateErr
	}

	_ = s.repo.AppendAuditLog(ctx, trimmed, "LIFECYCLE_TRANSITION", tenant.SourceService, tenant.ExternalID, tenant.CorrelationID, map[string]interface{}{
		"from_state": tenant.LifecycleState,
		"to_state":   nextState,
		"reason":     raw.Reason,
	})

	tenant.LifecycleState = nextState
	tenant.Version++
	tenant.UpdatedAt = time.Now().UTC()
	return tenant, nil
}

func (s *MultiTenantService) VerifyIngressEnvelope(ctx context.Context, raw contracts.VerifyEnvelopeInput) error {
	norm, err := contracts.NormalizeVerifyEnvelopeInput(raw)
	if err != nil {
		return err
	}

	tenant, tErr := s.repo.GetTenant(ctx, norm.HeaderTenant)
	if tErr != nil {
		return tErr
	}

	if tenant.LifecycleState == contracts.LifecycleSuspended {
		return rules.ErrTenantSuspended
	}

	canProcess, shouldSkip, lifeErr := rules.EvaluateLifecycleState(tenant.LifecycleState)
	if lifeErr != nil {
		if shouldSkip {
			return lifeErr
		}
		return lifeErr
	}
	if !canProcess {
		return errors.NewConflictError("tenant lifecycle state prevents processing")
	}

	if envErr := rules.ValidateEnvelopeConsistency(norm); envErr != nil {
		_ = s.repo.AppendAuditLog(ctx, norm.HeaderTenant, "INTEGRITY_MISMATCH_QUARANTINE", tenant.SourceService, tenant.ExternalID, tenant.CorrelationID, map[string]interface{}{
			"error": envErr.Error(),
		})
		return envErr
	}

	if resErr := rules.ValidateResidency(tenant.AllowedRegions, norm.CurrentRegion); resErr != nil {
		_ = s.repo.AppendAuditLog(ctx, norm.HeaderTenant, "RESIDENCY_VIOLATION_QUARANTINE", tenant.SourceService, tenant.ExternalID, tenant.CorrelationID, map[string]interface{}{
			"current_region":  norm.CurrentRegion,
			"allowed_regions": tenant.AllowedRegions,
		})
		return resErr
	}

	return nil
}

func (s *MultiTenantService) StoreSubjectDEK(ctx context.Context, raw contracts.StoreDEKInput) (*contracts.DEKRecord, error) {
	norm, err := contracts.NormalizeStoreDEKInput(raw)
	if err != nil {
		return nil, err
	}

	_, tErr := s.repo.GetTenant(ctx, norm.TenantID)
	if tErr != nil {
		return nil, tErr
	}

	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("dek_%s", strings.ReplaceAll(id, "-", "")[:20])
	now := time.Now().UTC()

	rec := contracts.DEKRecord{
		ID:            id,
		PublicID:      publicID,
		TenantID:      norm.TenantID,
		SourceService: norm.SourceService,
		ExternalID:    norm.ExternalID,
		CorrelationID: norm.CorrelationID,
		SubjectID:     norm.SubjectID,
		PeriodKey:     norm.PeriodKey,
		WrappedDEK:    norm.WrappedDEK,
		KEKKeyID:      norm.KEKKeyID,
		Algorithm:     norm.Algorithm,
		Status:        contracts.KeyStatusActive,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if storeErr := s.crypto.StoreDEK(ctx, rec); storeErr != nil {
		return nil, storeErr
	}

	_ = s.repo.AppendAuditLog(ctx, norm.TenantID, "DEK_STORED", norm.SourceService, norm.ExternalID, norm.CorrelationID, map[string]interface{}{
		"subject_id": norm.SubjectID,
		"period_key": norm.PeriodKey,
	})

	return &rec, nil
}

func (s *MultiTenantService) ShredSubjectDEK(ctx context.Context, tenantID string, subjectID string, periodKey string) error {
	tID := strings.TrimSpace(tenantID)
	sID := strings.TrimSpace(subjectID)
	pKey := strings.TrimSpace(periodKey)

	tenant, err := s.repo.GetTenant(ctx, tID)
	if err != nil {
		return err
	}

	if shredErr := s.crypto.ShredDEK(ctx, tID, sID, pKey); shredErr != nil {
		return shredErr
	}

	_ = s.repo.AppendAuditLog(ctx, tID, "KEY_SHREDDED", tenant.SourceService, tenant.ExternalID, tenant.CorrelationID, map[string]interface{}{
		"subject_id": sID,
		"period_key": pKey,
	})

	return nil
}

func (s *MultiTenantService) EncryptSubjectField(ctx context.Context, tenantID string, subjectID string, periodKey string, plaintext []byte) ([]byte, error) {
	return s.crypto.EncryptField(ctx, tenantID, subjectID, periodKey, plaintext)
}

func (s *MultiTenantService) DecryptSubjectField(ctx context.Context, tenantID string, subjectID string, periodKey string, ciphertext []byte) ([]byte, error) {
	return s.crypto.DecryptField(ctx, tenantID, subjectID, periodKey, ciphertext)
}

func (s *MultiTenantService) CheckQuota(ctx context.Context, tenantID string) (*contracts.QuotaCheckResult, error) {
	tID := strings.TrimSpace(tenantID)
	return s.repo.GetQuotaBudget(ctx, tID)
}
