package eventschemaregistry

import (
	"event-driven-platform/src/shared/errors"
)

func ValidateRegisterSchemaInput(input RegisterSchemaInput) error {
	if input.ID == "" {
		return errors.NewValidationError("id is required")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId is required")
	}
	if input.Subject == "" {
		return errors.NewValidationError("subject is required")
	}
	if input.Schema == "" {
		return errors.NewValidationError("schema content is required")
	}
	return nil
}
