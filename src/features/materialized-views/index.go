package materializedviews

import (
	"database/sql"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/infrastructure/adapters"
	"event-driven-platform/src/features/materialized-views/infrastructure/ports"
	"event-driven-platform/src/features/materialized-views/service"
)

type ViewService = service.ViewService
type ViewRepositoryPort = ports.ViewRepositoryPort

type ViewRegistryRecord = contracts.ViewRegistryRecord
type ProjectionStateRecord = contracts.ProjectionStateRecord
type HeldEventRecord = contracts.HeldEventRecord
type RebuildLogRecord = contracts.RebuildLogRecord

type RegisterViewInput = contracts.RegisterViewInput
type ApplyProjectionInput = contracts.ApplyProjectionInput
type InitiateRebuildInput = contracts.InitiateRebuildInput
type AdvanceRebuildInput = contracts.AdvanceRebuildInput
type ProjectionEvaluation = contracts.ProjectionEvaluation

func NewViewService(repo ports.ViewRepositoryPort) *service.ViewService {
	return service.NewViewService(repo)
}

func NewService(db *sql.DB) *service.ViewService {
	repo := adapters.NewPostgresViewRepositoryAdapter(db)
	return service.NewViewService(repo)
}

func NewPostgresViewRepositoryAdapter(db *sql.DB) ports.ViewRepositoryPort {
	return adapters.NewPostgresViewRepositoryAdapter(db)
}
