package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	cdcFeature "event-driven-platform/src/features/cdc-integration"
)

func RegisterCDCRoutes(mux *http.ServeMux, db *sql.DB) {
	feat := cdcFeature.NewCDCIntegrationFeature(db)
	h := handlers.NewCDCHandler(feat)

	mux.HandleFunc("POST /api/v1/cdc/connectors", h.HandleRegisterConnector)
	mux.HandleFunc("GET /api/v1/cdc/connectors/{name}", h.HandleGetConnector)
	mux.HandleFunc("POST /api/v1/cdc/signals", h.HandleDispatchSignal)
	mux.HandleFunc("POST /api/v1/cdc/heartbeat", h.HandleHeartbeat)
}

