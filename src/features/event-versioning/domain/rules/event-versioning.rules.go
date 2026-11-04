package rules

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"event-driven-platform/src/features/event-versioning/contracts"
)

var (
	ErrInvalidTenantID          = errors.New("tenantId must not be blank")
	ErrInvalidSourceService     = errors.New("sourceService must not be blank")
	ErrInvalidExternalID        = errors.New("externalId must not be blank")
	ErrInvalidStreamName        = errors.New("streamName must not be blank")
	ErrInvalidEventType         = errors.New("eventType must not be blank")
	ErrInvalidSchemaVersion     = errors.New("schemaVersion must be positive integer >= 1")
	ErrInvalidSchemaID          = errors.New("schemaId must not be blank")
	ErrUnsupportedFormat        = errors.New("format must be JSON_SCHEMA, AVRO, or PROTOBUF")
	ErrUnsupportedCompatibility = errors.New("compatibilityMode must be FULL_TRANSITIVE, BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, or NONE")
	ErrEmptySchemaDefinition    = errors.New("schemaDefinition must be valid non-empty json object")
	ErrIncompatibleSchema       = errors.New("schema breaks transitive compatibility rules")
	ErrNonSequentialUpcaster    = errors.New("upcaster toVersion must be strictly fromVersion + 1")
	ErrEmptyTransformerName     = errors.New("transformerName must not be blank")
)

func ValidateRegisterSchemaInput(in contracts.RegisterSchemaVersionInput) error {
	if strings.TrimSpace(in.TenantID) == "" {
		return ErrInvalidTenantID
	}
	if strings.TrimSpace(in.SourceService) == "" {
		return ErrInvalidSourceService
	}
	if strings.TrimSpace(in.ExternalID) == "" {
		return ErrInvalidExternalID
	}
	if strings.TrimSpace(in.StreamName) == "" {
		return ErrInvalidStreamName
	}
	if strings.TrimSpace(in.EventType) == "" {
		return ErrInvalidEventType
	}
	if in.SchemaVersion < 1 {
		return ErrInvalidSchemaVersion
	}
	if strings.TrimSpace(in.SchemaID) == "" {
		return ErrInvalidSchemaID
	}
	switch in.Format {
	case contracts.FormatJSONSchema, contracts.FormatAvro, contracts.FormatProtobuf:
	default:
		return ErrUnsupportedFormat
	}
	switch in.CompatibilityMode {
	case contracts.CompatFullTransitive, contracts.CompatBackwardTransitive, contracts.CompatForwardTransitive, contracts.CompatNone:
	default:
		return ErrUnsupportedCompatibility
	}
	if len(in.SchemaDefinition) == 0 {
		return ErrEmptySchemaDefinition
	}
	var testMap map[string]any
	if err := json.Unmarshal(in.SchemaDefinition, &testMap); err != nil || testMap == nil {
		return ErrEmptySchemaDefinition
	}
	return nil
}

func ValidateUpcasterDefinition(in contracts.RegisterUpcasterInput) error {
	if strings.TrimSpace(in.TenantID) == "" {
		return ErrInvalidTenantID
	}
	if strings.TrimSpace(in.SourceService) == "" {
		return ErrInvalidSourceService
	}
	if strings.TrimSpace(in.ExternalID) == "" {
		return ErrInvalidExternalID
	}
	if strings.TrimSpace(in.EventType) == "" {
		return ErrInvalidEventType
	}
	if in.FromVersion < 1 {
		return ErrInvalidSchemaVersion
	}
	if in.ToVersion != in.FromVersion+1 {
		return ErrNonSequentialUpcaster
	}
	if strings.TrimSpace(in.TransformerName) == "" {
		return ErrEmptyTransformerName
	}
	return nil
}

func EvaluateJSONSchemaCompatibility(
	prevSchemaDef json.RawMessage,
	newSchemaDef json.RawMessage,
	mode contracts.CompatibilityMode,
) contracts.CompatibilityCheckResult {
	if mode == contracts.CompatNone {
		return contracts.CompatibilityCheckResult{IsCompatible: true}
	}

	var prevMap, newMap map[string]any
	if err := json.Unmarshal(prevSchemaDef, &prevMap); err != nil {
		return contracts.CompatibilityCheckResult{
			IsCompatible: false,
			Violations:   []string{"previous schema is invalid json"},
		}
	}
	if err := json.Unmarshal(newSchemaDef, &newMap); err != nil {
		return contracts.CompatibilityCheckResult{
			IsCompatible: false,
			Violations:   []string{"new schema is invalid json"},
		}
	}

	violations := make([]string, 0)

	prevProps, _ := prevMap["properties"].(map[string]any)
	newProps, _ := newMap["properties"].(map[string]any)

	prevReqList := extractRequiredFields(prevMap)
	newReqList := extractRequiredFields(newMap)

	if mode == contracts.CompatBackwardTransitive || mode == contracts.CompatFullTransitive {
		for reqField := range newReqList {
			if _, existed := prevProps[reqField]; !existed {
				violations = append(violations, fmt.Sprintf("cannot add new required field %q in backward compatible schema", reqField))
			}
		}
	}

	if mode == contracts.CompatForwardTransitive || mode == contracts.CompatFullTransitive {
		for prevReq := range prevReqList {
			if _, stillExists := newProps[prevReq]; !stillExists {
				violations = append(violations, fmt.Sprintf("cannot delete required field %q in forward compatible schema", prevReq))
			}
		}
	}

	if len(violations) > 0 {
		return contracts.CompatibilityCheckResult{
			IsCompatible: false,
			Violations:   violations,
		}
	}

	return contracts.CompatibilityCheckResult{
		IsCompatible: true,
	}
}

func extractRequiredFields(schemaMap map[string]any) map[string]struct{} {
	out := make(map[string]struct{})
	reqSlice, ok := schemaMap["required"].([]any)
	if !ok {
		return out
	}
	for _, item := range reqSlice {
		if s, ok := item.(string); ok && s != "" {
			out[s] = struct{}{}
		}
	}
	return out
}

