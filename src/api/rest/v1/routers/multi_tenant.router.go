package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	multiTenantFeature "event-driven-platform/src/features/multi-tenant-event-isolation"
)

func RegisterMultiTenantRoutes(mux *http.ServeMux, db *sql.DB) {
	feat := multiTenantFeature.NewMultiTenantFeature(db)
	h := handlers.NewMultiTenantHandler(feat)

	mux.HandleFunc("POST /api/v1/tenants", h.HandleRegisterTenant)
	mux.HandleFunc("GET /api/v1/tenants/{tenant_id}", h.HandleGetTenant)
	mux.HandleFunc("PUT /api/v1/tenants/{tenant_id}/lifecycle", h.HandleTransitionLifecycle)
	mux.HandleFunc("POST /api/v1/tenants/verify-envelope", h.HandleVerifyEnvelope)
}

