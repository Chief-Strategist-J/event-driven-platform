package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type EventOrderingRouter struct {
	handler *handlers.EventOrderingHandler
}

func NewEventOrderingRouter(handler *handlers.EventOrderingHandler) *EventOrderingRouter {
	return &EventOrderingRouter{handler: handler}
}

func (r *EventOrderingRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/events/evaluate-sequence", r.handler.EvaluateSequence)
	mux.HandleFunc("POST /api/v1/events/held/drain", r.handler.DrainHeld)
	mux.HandleFunc("GET /api/v1/events/held", r.handler.ListHeld)
}
