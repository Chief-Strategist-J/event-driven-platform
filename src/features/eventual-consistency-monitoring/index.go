package eventualconsistencymonitoring

import (
	"database/sql"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/adapters"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/ports"
	"event-driven-platform/src/features/eventual-consistency-monitoring/service"
)

type ConsistencyService = service.ConsistencyService
type ConsistencyRepositoryPort = ports.ConsistencyRepositoryPort

type WatermarkRecord = contracts.WatermarkRecord
type DriftIncidentRecord = contracts.DriftIncidentRecord
type CheckRunRecord = contracts.CheckRunRecord
type SilenceRecord = contracts.SilenceRecord

type ObserveWatermarkInput = contracts.ObserveWatermarkInput
type EvaluateRecordInput = contracts.EvaluateRecordInput
type EvaluationResult = contracts.EvaluationResult
type RegisterSilenceInput = contracts.RegisterSilenceInput
type CanaryProbeInput = contracts.CanaryProbeInput
type CanaryProbeOutput = contracts.CanaryProbeOutput
type RunCheckSweepInput = contracts.RunCheckSweepInput

func NewConsistencyService(repo ports.ConsistencyRepositoryPort) *service.ConsistencyService {
	return service.NewConsistencyService(repo)
}

func NewService(db *sql.DB) *service.ConsistencyService {
	repo := adapters.NewPostgresConsistencyRepositoryAdapter(db)
	return service.NewConsistencyService(repo)
}

func NewPostgresConsistencyRepositoryAdapter(db *sql.DB) ports.ConsistencyRepositoryPort {
	return adapters.NewPostgresConsistencyRepositoryAdapter(db)
}

