package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	offsetFeature "event-driven-platform/src/features/offset-management"
	"event-driven-platform/src/features/offset-management/infrastructure/ports"
)

func RegisterOffsetManagementRoutes(mux *http.ServeMux, repo ports.OffsetManagementRepositoryPort) {
	feat := offsetFeature.NewOffsetManagementFeature(repo)
	h := handlers.NewOffsetManagementHandler(feat)

	mux.HandleFunc("POST /api/v1/offsets/commit", h.HandleCommitOffset)
	mux.HandleFunc("POST /api/v1/offsets/rewind", h.HandleRewindOffset)
	mux.HandleFunc("POST /api/v1/offsets/fast-forward", h.HandleFastForwardOffset)
	mux.HandleFunc("GET /api/v1/offsets/lag", h.HandleEvaluateLag)
}

