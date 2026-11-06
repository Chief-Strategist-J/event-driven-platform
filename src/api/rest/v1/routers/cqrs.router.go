package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	cqrsfeature "event-driven-platform/src/features/cqrs"
)

func RegisterCQRSRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := cqrsfeature.NewService(db)
	h := handlers.NewCQRSHandler(svc)

	mux.HandleFunc("POST /api/v1/commands/dispatch", h.HandleDispatchCommand)
	mux.HandleFunc("GET /api/v1/queries/resolve", h.HandleResolveQuery)
	mux.HandleFunc("POST /api/v1/commands/keys", h.HandleRegisterSigningKey)
	mux.HandleFunc("GET /api/v1/queries/authoritative-own", h.HandleAuthoritativeRead)
}
