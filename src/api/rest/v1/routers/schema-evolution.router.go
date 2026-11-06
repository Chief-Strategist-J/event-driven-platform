package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	schemaevolution "event-driven-platform/src/features/schema-evolution"
)

func RegisterSchemaEvolutionRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := schemaevolution.NewService(db)
	h := handlers.NewSchemaEvolutionHandler(svc)

	mux.HandleFunc("POST /api/v1/schemas/register", h.HandleRegisterSchema)
	mux.HandleFunc("GET /api/v1/schemas/{subject}/readiness", h.HandleCheckConsumerReadiness)
	mux.HandleFunc("POST /api/v1/schemas/dual-publish", h.HandleInitiateDualPublish)
	mux.HandleFunc("POST /api/v1/schemas/decommission/check", h.HandleEvaluateDecommission)
}
