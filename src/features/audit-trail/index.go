package audittrail

import (
	"database/sql"

	"event-driven-platform/src/features/audit-trail/contracts"
	"event-driven-platform/src/features/audit-trail/infrastructure/adapters"
	"event-driven-platform/src/features/audit-trail/infrastructure/ports"
	"event-driven-platform/src/features/audit-trail/service"
)

type AuditTrailService = service.AuditTrailService
type AuditTrailRepositoryPort = ports.AuditTrailRepositoryPort

type AuditRecord = contracts.AuditRecord
type CheckpointRecord = contracts.CheckpointRecord
type AccessLogRecord = contracts.AccessLogRecord
type VerificationReport = contracts.VerificationReport

type IngestAuditRecordInput = contracts.IngestAuditRecordInput
type CreateCheckpointInput = contracts.CreateCheckpointInput
type VerifyShardIntegrityInput = contracts.VerifyShardIntegrityInput
type QueryAuditInput = contracts.QueryAuditInput

func NewAuditTrailService(repo ports.AuditTrailRepositoryPort, optionalSecret ...string) *service.AuditTrailService {
	return service.NewAuditTrailService(repo, optionalSecret...)
}

func NewService(db *sql.DB, optionalSecret ...string) *service.AuditTrailService {
	repo := adapters.NewPostgresAuditTrailRepositoryAdapter(db)
	return service.NewAuditTrailService(repo, optionalSecret...)
}

func NewPostgresAuditTrailRepositoryAdapter(db *sql.DB) ports.AuditTrailRepositoryPort {
	return adapters.NewPostgresAuditTrailRepositoryAdapter(db)
}
