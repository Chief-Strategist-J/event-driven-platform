package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type RetryTopicsRouter struct {
	handler *handlers.RetryTopicsHandler
}

func NewRetryTopicsRouter(handler *handlers.RetryTopicsHandler) *RetryTopicsRouter {
	return &RetryTopicsRouter{handler: handler}
}

func (r *RetryTopicsRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/retries/evaluate", r.handler.EvaluateRetry)
	mux.HandleFunc("POST /api/v1/retries/attempts", r.handler.ScheduleRetry)
	mux.HandleFunc("GET /api/v1/retries/due", r.handler.GetDueRetries)
}
