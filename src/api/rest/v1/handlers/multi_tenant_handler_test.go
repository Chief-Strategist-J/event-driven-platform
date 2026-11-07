package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	appErrors "event-driven-platform/src/shared/errors"
)

func TestMapTenantError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"validation", appErrors.NewValidationError("invalid tenancy tier: X"), http.StatusBadRequest},
		{"not found", appErrors.NewNotFoundError("tenant not found: x"), http.StatusNotFound},
		{"conflict wrapped", fmt.Errorf("%w: cannot transition", appErrors.NewConflictError("invalid tenant lifecycle transition")), http.StatusConflict},
		{"untyped", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		if got, _ := mapTenantError(c.err); got != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, got)
		}
	}
}
