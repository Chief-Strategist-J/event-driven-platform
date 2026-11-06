package eventdrivencommunication

import (
	"database/sql"

	"event-driven-platform/src/features/event-driven-communication/infrastructure/adapters"
	"event-driven-platform/src/features/event-driven-communication/service"
)

func NewService(db *sql.DB) *service.EventCommunicationService {
	repo := adapters.NewPostgresEventCommunicationAdapter(db)
	return service.NewEventCommunicationService(repo)
}

