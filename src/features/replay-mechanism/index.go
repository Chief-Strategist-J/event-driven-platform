package replay

import (
	"event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
	"event-driven-platform/src/features/replay-mechanism/service"
)

type ReplayFeature struct {
	Service *service.ReplayService
}

func NewReplayFeature(repo ports.ReplayRepositoryPort) *ReplayFeature {
	return &ReplayFeature{
		Service: service.NewReplayService(repo),
	}
}

