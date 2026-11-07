package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type EventVersioningRouter struct {
	handler *handlers.EventVersioningHandler
}

func NewEventVersioningRouter(handler *handlers.EventVersioningHandler) *EventVersioningRouter {
	return &EventVersioningRouter{handler: handler}
}

func (r *EventVersioningRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/schemas/versions", r.handler.RegisterSchema)
	mux.HandleFunc("POST /api/v1/schemas/compatibility/check", r.handler.CheckCompatibility)
	mux.HandleFunc("POST /api/v1/schemas/upcasters", r.handler.RegisterUpcaster)
	mux.HandleFunc("POST /api/v1/schemas/upcast", r.handler.Upcast)
}
