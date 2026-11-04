package contracts

import (
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrValidationFailed = errors.New("schema validation failed")
)

func ValidateRawJSON(data json.RawMessage) error {
	if len(data) == 0 {
		return ErrValidationFailed
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil || m == nil {
		return ErrValidationFailed
	}
	return nil
}

func NormalizeString(s string) string {
	return strings.TrimSpace(s)
}
