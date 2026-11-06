package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	partitioning "event-driven-platform/src/features/partitioning-strategy"
)

func RegisterPartitioningRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := partitioning.NewService(db)
	h := handlers.NewPartitioningHandler(svc)

	mux.HandleFunc("POST /api/v1/partitioning/topologies", h.HandleRegisterTopology)
	mux.HandleFunc("POST /api/v1/partitioning/keys/calculate", h.HandleCalculatePartition)
	mux.HandleFunc("POST /api/v1/partitioning/skew/evaluate", h.HandleEvaluateSkew)
	mux.HandleFunc("POST /api/v1/partitioning/migrations", h.HandleInitiateMigration)
	mux.HandleFunc("PUT /api/v1/partitioning/migrations/phase", h.HandleAdvanceMigration)
}

