package multitenant

import (
	"database/sql"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
	"event-driven-platform/src/features/multi-tenant-event-isolation/infrastructure/adapters"
	"event-driven-platform/src/features/multi-tenant-event-isolation/infrastructure/ports"
	"event-driven-platform/src/features/multi-tenant-event-isolation/service"
)

type MultiTenantFeature struct {
	Service *service.MultiTenantService
	Repo    ports.TenantRepositoryPort
	Crypto  ports.CryptoKeystorePort
}

func NewMultiTenantFeature(db *sql.DB) *MultiTenantFeature {
	repo := adapters.NewPostgresTenantRepositoryAdapter(db)
	crypto := adapters.NewInMemoryCryptoAdapter()
	machine := machines.NewTenantLifecycleMachine()
	svc := service.NewMultiTenantService(repo, crypto, machine)

	return &MultiTenantFeature{
		Service: svc,
		Repo:    repo,
		Crypto:  crypto,
	}
}

type (
	RegisterTenantInput  = contracts.RegisterTenantInput
	UpdateLifecycleInput = contracts.UpdateLifecycleInput
	StoreDEKInput        = contracts.StoreDEKInput
	VerifyEnvelopeInput  = contracts.VerifyEnvelopeInput
	TenantRecord         = contracts.TenantRecord
	DEKRecord            = contracts.DEKRecord
	QuotaCheckResult     = contracts.QuotaCheckResult
)

