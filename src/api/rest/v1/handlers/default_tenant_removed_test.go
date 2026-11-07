package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// Routes that persist or read tenant data must not fall back to a "default" tenant.
func TestHealAndReadinessRequireTenantID(t *testing.T) {
	heal := httptest.NewRequest(http.MethodPost, "/api/v1/consistency/incidents/x/heal", nil)
	heal.SetPathValue("id", uuid.NewString())
	rec := httptest.NewRecorder()
	(&ConsistencyMonitoringHandler{}).HandleAttemptHealing(rec, heal)
	assertBadRequest(t, rec, "tenant_id")

	ready := httptest.NewRequest(http.MethodGet, "/api/v1/schemas/s/readiness?target_version=1", nil)
	ready.SetPathValue("subject", "s")
	rec = httptest.NewRecorder()
	(&SchemaEvolutionHandler{}).HandleCheckConsumerReadiness(rec, ready)
	assertBadRequest(t, rec, "tenant_id")
}
