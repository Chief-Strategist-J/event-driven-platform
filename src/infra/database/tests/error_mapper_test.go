package tests

import (
	"errors"
	"testing"

	"github.com/lib/pq"

	"event-driven-platform/src/infra/database"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
)

func TestDatabaseErrorMapper_MapsNamedConstraintsToApplicationErrors(t *testing.T) {
	testCases := []struct {
		name               string
		pqErr              *pq.Error
		expectedStatusCode int
		expectedCode       string
	}{
		{
			name: "Consumer inbox dedupe violation",
			pqErr: &pq.Error{
				Code:       "23505",
				Constraint: constants.ConstraintConsumerInboxDedupeUK,
			},
			expectedStatusCode: 409,
			expectedCode:       constants.ErrCodeConflict,
		},
		{
			name: "Consumer inbox unnormalized tenant check",
			pqErr: &pq.Error{
				Code:       "23514",
				Constraint: constants.ConstraintConsumerInboxTenantIDCK,
			},
			expectedStatusCode: 400,
			expectedCode:       constants.ErrCodeValidationFailed,
		},
		{
			name: "Consumer inbox bad payload hash check",
			pqErr: &pq.Error{
				Code:       "23514",
				Constraint: constants.ConstraintConsumerInboxPayloadHashCK,
			},
			expectedStatusCode: 400,
			expectedCode:       constants.ErrCodeValidationFailed,
		},
		{
			name: "Consumer inbox invalid version check",
			pqErr: &pq.Error{
				Code:       "23514",
				Constraint: constants.ConstraintConsumerInboxAggregateVersionCK,
			},
			expectedStatusCode: 400,
			expectedCode:       constants.ErrCodeValidationFailed,
		},
		{
			name: "Unmapped 23xxx integrity violation falls back to conflict",
			pqErr: &pq.Error{
				Code:       "23503",
				Constraint: "fk_some_foreign_table",
			},
			expectedStatusCode: 409,
			expectedCode:       constants.ErrCodeConflict,
		},
		{
			name: "Connection exception class 08 maps to timeout",
			pqErr: &pq.Error{
				Code: "08006",
			},
			expectedStatusCode: 504,
			expectedCode:       "TIMEOUT",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := database.MapDatabaseError(tc.pqErr)
			var appErr *appErrors.ApplicationError
			if !errors.As(res, &appErr) {
				t.Fatalf("expected ApplicationError, got %T (%v)", res, res)
			}
			if appErr.StatusCode != tc.expectedStatusCode {
				t.Errorf("expected status %d, got %d", tc.expectedStatusCode, appErr.StatusCode)
			}
			if appErr.CanonicalCode != tc.expectedCode {
				t.Errorf("expected code %s, got %s", tc.expectedCode, appErr.CanonicalCode)
			}
		})
	}
}

func TestDatabaseErrorMapper_SanitizesUnknownErrors(t *testing.T) {
	genericErr := errors.New("fatal: server closed connection unexpectedly at 192.168.1.50:5432")
	res := database.MapDatabaseError(genericErr)

	var appErr *appErrors.ApplicationError
	if !errors.As(res, &appErr) {
		t.Fatalf("expected ApplicationError, got %T", res)
	}

	if appErr.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", appErr.StatusCode)
	}
	if appErr.Message != "internal database error" {
		t.Errorf("expected sanitized internal message, got %s", appErr.Message)
	}
}

