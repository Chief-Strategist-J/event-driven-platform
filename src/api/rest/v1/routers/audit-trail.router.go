package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	audittrail "event-driven-platform/src/features/audit-trail"
)

func RegisterAuditTrailRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := audittrail.NewService(db)
	h := handlers.NewAuditTrailHandler(svc)

	mux.HandleFunc("POST /api/v1/audits/ingest", h.HandleIngestAuditRecord)
	mux.HandleFunc("POST /api/v1/audits/checkpoints", h.HandleCreateCheckpoint)
	mux.HandleFunc("POST /api/v1/audits/verify", h.HandleVerifyShardIntegrity)
	mux.HandleFunc("GET /api/v1/audits/query", h.HandleQueryAudits)
}
