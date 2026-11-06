package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/features/event-operations-dashboard/infrastructure/adapters"
	"event-driven-platform/src/features/event-operations-dashboard/service"
)

func RegisterEventOperationsDashboardRoutes(mux *http.ServeMux, db *sql.DB) {
	repo := adapters.NewPostgresEventOperationsDashboardRepository(db)
	svc := service.NewEventOperationsDashboardService(repo)
	h := handlers.NewEventOperationsDashboardHandler(svc)

	mux.HandleFunc("POST /api/v1/ops/hops/report", h.HandleReportHopTelemetry)
	mux.HandleFunc("POST /api/v1/ops/triage/localize", h.HandleTriageFlow)
}
