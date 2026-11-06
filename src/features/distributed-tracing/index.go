package distributedtracing

import (
	"database/sql"

	"event-driven-platform/src/features/distributed-tracing/infrastructure/adapters"
	"event-driven-platform/src/features/distributed-tracing/service"
)

func NewService(db *sql.DB) *service.DistributedTracingService {
	repo := adapters.NewPostgresDistributedTracingAdapter(db)
	return service.NewDistributedTracingService(repo)
}

