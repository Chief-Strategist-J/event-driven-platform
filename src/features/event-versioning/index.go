package event_versioning

import (
	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/domain/machines"
	"event-driven-platform/src/features/event-versioning/infrastructure/ports"
	"event-driven-platform/src/features/event-versioning/service"
)

type EventVersioningService = service.EventVersioningService
type EventVersioningRepositoryPort = ports.EventVersioningRepositoryPort
type SchemaVersionRecord = contracts.SchemaVersionRecord
type UpcasterRecord = contracts.UpcasterRecord
type UpcastResult = contracts.UpcastResult
type UpcastTransformerFunc = machines.UpcastTransformerFunc

func NewEventVersioningService(repo ports.EventVersioningRepositoryPort, engine *machines.UpcasterEngine) *service.EventVersioningService {
	return service.NewEventVersioningService(repo, engine)
}

func NewUpcasterEngine() *machines.UpcasterEngine {
	return machines.NewUpcasterEngine()
}

