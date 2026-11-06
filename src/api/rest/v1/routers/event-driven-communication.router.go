package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	eventdrivencommunication "event-driven-platform/src/features/event-driven-communication"
)

func RegisterEventCommunicationRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := eventdrivencommunication.NewService(db)
	h := handlers.NewEventCommunicationHandler(svc)

	mux.HandleFunc("POST /api/v1/communication/sagas/start", h.HandleStartSaga)
	mux.HandleFunc("POST /api/v1/communication/sagas/reply", h.HandleProcessStepReply)
	mux.HandleFunc("POST /api/v1/communication/sagas/compensate", h.HandleTriggerCompensation)
	mux.HandleFunc("POST /api/v1/communication/commands/dispatch", h.HandleDispatchAsyncCommand)
}

