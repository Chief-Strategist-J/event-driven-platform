package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type EventSchemaRegistryRouter struct {
	handler *handlers.EventSchemaRegistryHandler
}

func NewEventSchemaRegistryRouter(h *handlers.EventSchemaRegistryHandler) *EventSchemaRegistryRouter {
	return &EventSchemaRegistryRouter{handler: h}
}

func (r *EventSchemaRegistryRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schemas", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.RegisterSchema(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}
