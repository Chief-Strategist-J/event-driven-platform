package idempotentconsumers

import (
	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/adapters"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/ports"
	"event-driven-platform/src/features/idempotent-consumers/service"
)

type ConsumerService = service.ConsumerService
type InboundConsumerEvent = contracts.InboundConsumerEvent
type ConsumeResult = contracts.ConsumeResult
type ConsumerProcessAction = contracts.ConsumerProcessAction

func NewService(repo ports.ConsumerRepositoryPort) *ConsumerService {
	return service.NewConsumerService(repo)
}

func NewMemoryRepositoryAdapter() ports.ConsumerRepositoryPort {
	return adapters.NewMemoryConsumerRepositoryAdapter()
}

