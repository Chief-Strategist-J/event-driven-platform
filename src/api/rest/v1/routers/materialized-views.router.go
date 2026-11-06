package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	materializedviews "event-driven-platform/src/features/materialized-views"
)

func RegisterMaterializedViewsRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := materializedviews.NewService(db)
	h := handlers.NewMaterializedViewsHandler(svc)

	mux.HandleFunc("POST /api/v1/materialized-views", h.HandleRegisterView)
	mux.HandleFunc("GET /api/v1/materialized-views/{id}", h.HandleGetViewByID)
	mux.HandleFunc("POST /api/v1/materialized-views/apply", h.HandleApplyProjection)
	mux.HandleFunc("POST /api/v1/materialized-views/rebuild", h.HandleInitiateRebuild)
	mux.HandleFunc("PUT /api/v1/materialized-views/rebuild/phase", h.HandleAdvanceRebuild)
	mux.HandleFunc("POST /api/v1/materialized-views/contributions", h.HandleApplyContribution)
	mux.HandleFunc("POST /api/v1/materialized-views/periods", h.HandleRecordPeriodEvent)
	mux.HandleFunc("POST /api/v1/materialized-views/query", h.HandleQueryView)
}
