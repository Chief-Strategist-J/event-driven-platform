package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	distributedtracing "event-driven-platform/src/features/distributed-tracing"
)

func RegisterDistributedTracingRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := distributedtracing.NewService(db)
	h := handlers.NewDistributedTracingHandler(svc)

	mux.HandleFunc("POST /api/v1/tracing/context/inject", h.HandleInjectTraceContext)
	mux.HandleFunc("POST /api/v1/tracing/context/extract", h.HandleExtractTraceContext)
	mux.HandleFunc("POST /api/v1/tracing/sampling/evaluate", h.HandleEvaluateSampling)
	mux.HandleFunc("POST /api/v1/tracing/coverage/report", h.HandleReportCoverage)
}

