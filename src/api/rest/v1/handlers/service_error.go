package handlers

import (
	"errors"
	"strings"

	appErrors "event-driven-platform/src/shared/errors"
)

// classifyServiceError maps an error returned by a feature service to an HTTP
// status, canonical code and client-safe message. ApplicationErrors keep their
// own status; the generic database-constraint fallbacks are rewritten so raw
// constraint names are not leaked to clients. Any other error uses the fallback.
func classifyServiceError(err error, fallbackStatus int, fallbackCode string) (int, string, string) {
	var appErr *appErrors.ApplicationError
	if errors.As(err, &appErr) {
		msg := appErr.Message
		if strings.HasPrefix(msg, "database integrity constraint violation") {
			msg = "request conflicts with existing data or violates a data integrity rule"
		}
		return appErr.StatusCode, appErr.CanonicalCode, msg
	}
	return fallbackStatus, fallbackCode, err.Error()
}
