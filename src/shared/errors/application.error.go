package errors

import (
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
)

type ApplicationError struct {
	StatusCode    int                         `json:"statusCode"`
	CanonicalCode string                      `json:"code"`
	Message       string                      `json:"message"`
	Retryable     bool                        `json:"retryable"`
	Details       []types.ResponseErrorDetail `json:"details,omitempty"`
}

func (e *ApplicationError) Error() string {
	return e.Message
}

func NewValidationError(message string, details ...types.ResponseErrorDetail) *ApplicationError {
	return &ApplicationError{
		StatusCode:    400,
		CanonicalCode: constants.ErrCodeValidationFailed,
		Message:       message,
		Retryable:     false,
		Details:       details,
	}
}

func NewUnauthorizedError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    401,
		CanonicalCode: "UNAUTHORIZED",
		Message:       message,
		Retryable:     false,
	}
}

func NewForbiddenError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    403,
		CanonicalCode: "FORBIDDEN",
		Message:       message,
		Retryable:     false,
	}
}

func NewNotFoundError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    404,
		CanonicalCode: constants.ErrCodeNotFound,
		Message:       message,
		Retryable:     false,
	}
}

func NewConflictError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    409,
		CanonicalCode: constants.ErrCodeConflict,
		Message:       message,
		Retryable:     false,
	}
}

func NewPreconditionFailedError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    412,
		CanonicalCode: constants.ErrCodePreconditionFailed,
		Message:       message,
		Retryable:     false,
	}
}

func NewTimeoutError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    504,
		CanonicalCode: "TIMEOUT",
		Message:       message,
		Retryable:     true,
	}
}

func NewInternalError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    500,
		CanonicalCode: constants.ErrCodeInternalError,
		Message:       message,
		Retryable:     false,
	}
}
