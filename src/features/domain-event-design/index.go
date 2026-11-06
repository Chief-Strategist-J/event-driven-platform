package domaineventdesign

import (
	"database/sql"

	"event-driven-platform/src/features/domain-event-design/infrastructure/adapters"
	"event-driven-platform/src/features/domain-event-design/service"
)

func NewService(db *sql.DB) *service.DomainEventService {
	repo := adapters.NewPostgresDomainEventAdapter(db)
	return service.NewDomainEventService(repo)
}

