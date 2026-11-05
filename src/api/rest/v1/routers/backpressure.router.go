package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	bpFeature "event-driven-platform/src/features/backpressure"
	"event-driven-platform/src/features/backpressure/infrastructure/ports"
)

func RegisterBackpressureRoutes(mux *http.ServeMux, repo ports.BackpressureRepositoryPort) {
	feat := bpFeature.NewBackpressureFeature(repo)
	h := handlers.NewBackpressureHandler(feat)

	mux.HandleFunc("POST /api/v1/backpressure/saturation", h.HandleReportSaturation)
	mux.HandleFunc("POST /api/v1/backpressure/recovery", h.HandleReportRecovery)
	mux.HandleFunc("POST /api/v1/backpressure/recovery/confirm", h.HandleConfirmRecovery)
	mux.HandleFunc("GET /api/v1/backpressure/state", h.HandleGetState)
}

