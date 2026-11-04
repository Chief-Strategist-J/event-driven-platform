package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type DeadLetterRouter struct {
	handler *handlers.DeadLetterHandler
}

func NewDeadLetterRouter(h *handlers.DeadLetterHandler) *DeadLetterRouter {
	return &DeadLetterRouter{handler: h}
}

func (rt *DeadLetterRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/dlq/quarantine", rt.handler.Quarantine)
	mux.HandleFunc("POST /api/v1/dlq/redrive", rt.handler.Redrive)
	mux.HandleFunc("POST /api/v1/dlq/discard", rt.handler.Discard)
	mux.HandleFunc("GET /api/v1/dlq/quarantined", rt.handler.List)
}
