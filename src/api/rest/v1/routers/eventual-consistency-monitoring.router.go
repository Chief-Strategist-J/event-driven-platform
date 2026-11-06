package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	eventualconsistencymonitoring "event-driven-platform/src/features/eventual-consistency-monitoring"
)

func RegisterConsistencyMonitoringRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := eventualconsistencymonitoring.NewService(db)
	h := handlers.NewConsistencyMonitoringHandler(svc)

	mux.HandleFunc("POST /api/v1/consistency/watermarks", h.HandleObserveWatermark)
	mux.HandleFunc("GET /api/v1/consistency/views/{view_name}/watermark", h.HandleGetViewWatermarks)
	mux.HandleFunc("POST /api/v1/consistency/evaluate", h.HandleEvaluateRecord)
	mux.HandleFunc("GET /api/v1/consistency/views/{view_name}/incidents", h.HandleGetActiveIncidents)
	mux.HandleFunc("POST /api/v1/consistency/incidents/{id}/heal", h.HandleAttemptHealing)
	mux.HandleFunc("POST /api/v1/consistency/sweeps/run", h.HandleRecordCheckSweep)
	mux.HandleFunc("GET /api/v1/consistency/views/{view_name}/sweeps", h.HandleListCheckRuns)
	mux.HandleFunc("POST /api/v1/consistency/silences", h.HandleRegisterSilence)
	mux.HandleFunc("POST /api/v1/consistency/probes/canary", h.HandleDispatchCanaryProbe)
}

