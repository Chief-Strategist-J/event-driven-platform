package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	replayFeature "event-driven-platform/src/features/replay-mechanism"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
)

func RegisterReplayRoutes(mux *http.ServeMux, repo ports.ReplayRepositoryPort) {
	feat := replayFeature.NewReplayFeature(repo)
	h := handlers.NewReplayHandler(feat)

	mux.HandleFunc("POST /api/v1/replay/runs", h.HandleCreateReplayRun)
	mux.HandleFunc("POST /api/v1/replay/runs/start", h.HandleStartStreaming)
	mux.HandleFunc("POST /api/v1/replay/runs/advance", h.HandleAdvanceReplay)
	mux.HandleFunc("POST /api/v1/replay/runs/promote", h.HandlePromoteReplay)
	mux.HandleFunc("POST /api/v1/replay/runs/cutover", h.HandleCompleteCutover)
	mux.HandleFunc("POST /api/v1/replay/effects/evaluate", h.HandleEvaluateEffect)
	mux.HandleFunc("GET /api/v1/replay/runs", h.HandleGetReplayRun)
}

