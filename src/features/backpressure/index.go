package backpressure

import (
	"event-driven-platform/src/features/backpressure/service"
	"event-driven-platform/src/features/backpressure/infrastructure/ports"
)

type BackpressureFeature struct {
	Service *service.BackpressureService
}

func NewBackpressureFeature(repo ports.BackpressureRepositoryPort) *BackpressureFeature {
	return &BackpressureFeature{
		Service: service.NewBackpressureService(repo),
	}
}

