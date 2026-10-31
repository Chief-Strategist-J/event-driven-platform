package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type SchemaRegistryRouter struct {
	handler *handlers.SchemaRegistryHandler
}

func NewSchemaRegistryRouter(h *handlers.SchemaRegistryHandler) *SchemaRegistryRouter {
	return &SchemaRegistryRouter{handler: h}
}

func (r *SchemaRegistryRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schemas", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.RegisterSchema(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}
