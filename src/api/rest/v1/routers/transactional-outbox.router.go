package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type TransactionalOutboxRouter struct {
	handler *handlers.TransactionalOutboxHandler
}

func NewTransactionalOutboxRouter(h *handlers.TransactionalOutboxHandler) *TransactionalOutboxRouter {
	return &TransactionalOutboxRouter{handler: h}
}

func (rt *TransactionalOutboxRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/outbox", rt.handler.CreateOutboxRecord)
	mux.HandleFunc("POST /api/v1/outbox/relay", rt.handler.RelayBatch)
}
