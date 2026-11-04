package transactionaloutbox

import (
	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/ports"
	"event-driven-platform/src/features/transactional-outbox/service"
)

type OutboxService = service.OutboxService
type OutboxRecord = contracts.OutboxRecord
type OutboxStatus = contracts.OutboxStatus
type CreateOutboxRecordInput = contracts.CreateOutboxRecordInput
type OutboxRepositoryPort = ports.OutboxRepositoryPort

func NewService(repo ports.OutboxRepositoryPort, producer any) *service.OutboxService {
	if prod, ok := producer.(ports.OutboxRepositoryPort); ok {
		_ = prod
	}
	return service.NewOutboxService(repo, nil)
}

func NewMemoryRepositoryAdapter() ports.OutboxRepositoryPort {
	return adapters.NewMemoryOutboxRepositoryAdapter()
}

