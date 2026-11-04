package chassis

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type HealthCoordinator struct {
	isLive  atomic.Bool
	isReady atomic.Bool
}

func NewHealthCoordinator() *HealthCoordinator {
	h := &HealthCoordinator{}
	h.isLive.Store(true)
	h.isReady.Store(false)
	return h
}

func (h *HealthCoordinator) SetLive(val bool) {
	h.isLive.Store(val)
}

func (h *HealthCoordinator) SetReady(val bool) {
	h.isReady.Store(val)
}

func (h *HealthCoordinator) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !h.isLive.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "down"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
	}
}

func (h *HealthCoordinator) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !h.isReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	}
}
