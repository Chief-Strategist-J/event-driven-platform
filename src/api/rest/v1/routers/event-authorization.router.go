package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/features/event-authorization/infrastructure/adapters"
	"event-driven-platform/src/features/event-authorization/service"
)

func RegisterEventAuthorizationRoutes(mux *http.ServeMux, db *sql.DB) {
	repo := adapters.NewPostgresEventAuthorizationRepository(db)
	svc := service.NewEventAuthorizationService(repo)
	h := handlers.NewEventAuthorizationHandler(svc)

	mux.HandleFunc("POST /api/v1/auth/policies", h.HandleCreatePolicy)
	mux.HandleFunc("POST /api/v1/auth/evaluate", h.HandleEvaluateAccess)
}
