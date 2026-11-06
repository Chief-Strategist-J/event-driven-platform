package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	domaineventdesign "event-driven-platform/src/features/domain-event-design"
)

func RegisterDomainEventRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := domaineventdesign.NewService(db)
	h := handlers.NewDomainEventHandler(svc)

	mux.HandleFunc("POST /api/v1/domain-events/envelope/build", h.HandleBuildEnvelope)
	mux.HandleFunc("POST /api/v1/domain-events/catalog", h.HandleRegisterCatalogEvent)
	mux.HandleFunc("POST /api/v1/domain-events/aggregates/reserve-version", h.HandleReserveAggregateVersion)
	mux.HandleFunc("POST /api/v1/domain-events/causality/verify", h.HandleVerifyCausalChain)
}

