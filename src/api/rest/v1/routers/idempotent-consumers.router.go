package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type IdempotentConsumerRouter struct {
	handler *handlers.IdempotentConsumerHandler
}

func NewIdempotentConsumerRouter(h *handlers.IdempotentConsumerHandler) *IdempotentConsumerRouter {
	return &IdempotentConsumerRouter{handler: h}
}

func (rt *IdempotentConsumerRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/consumers/inbox", rt.handler.Consume)
}
