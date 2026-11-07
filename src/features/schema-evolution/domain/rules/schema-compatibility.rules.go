package rules

import (
	"encoding/json"
	"fmt"
	"sort"

	"event-driven-platform/src/features/schema-evolution/contracts"
)

type schemaField struct {
	required bool
	typ      string
}

// parseSchemaFields extracts field semantics from an Avro record or a JSON
// Schema object. ok is false when the definition is not a recognised JSON
// structure, in which case no semantic comparison is possible.
func parseSchemaFields(definition string) (fields map[string]schemaField, ok bool) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(definition), &doc); err != nil {
		return nil, false
	}
	fields = map[string]schemaField{}

	if raw, has := doc["fields"]; has {
		var avro []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &avro); err != nil {
			return nil, false
		}
		for _, f := range avro {
			var name string
			if err := json.Unmarshal(f["name"], &name); err != nil || name == "" {
				return nil, false
			}
			_, hasDefault := f["default"]
			fields[name] = schemaField{required: !hasDefault, typ: canonicalJSON(f["type"])}
		}
		return fields, true
	}

	if raw, has := doc["properties"]; has {
		var props map[string]json.RawMessage
		if err := json.Unmarshal(raw, &props); err != nil {
			return nil, false
		}
		required := map[string]bool{}
		if rraw, has := doc["required"]; has {
			var names []string
			if err := json.Unmarshal(rraw, &names); err != nil {
				return nil, false
			}
			for _, n := range names {
				required[n] = true
			}
		}
		for name, praw := range props {
			var prop map[string]json.RawMessage
			_ = json.Unmarshal(praw, &prop)
			_, hasDefault := prop["default"]
			fields[name] = schemaField{required: required[name] && !hasDefault, typ: canonicalJSON(prop["type"])}
		}
		return fields, true
	}

	// A bare object without fields/properties (e.g. {"type":"record"}) has no fields.
	return fields, true
}

func canonicalJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// backwardViolations: consumers using newDef must read data written with oldDef.
func backwardViolations(oldF, newF map[string]schemaField) []string {
	var out []string
	for name, nf := range newF {
		of, existed := oldF[name]
		if !existed {
			if nf.required {
				out = append(out, fmt.Sprintf("BACKWARD: new required field %q has no default", name))
			}
			continue
		}
		if of.typ != nf.typ {
			out = append(out, fmt.Sprintf("BACKWARD: field %q changed type from %s to %s", name, of.typ, nf.typ))
		}
	}
	return out
}

// forwardViolations: consumers using oldDef must read data written with newDef.
func forwardViolations(oldF, newF map[string]schemaField) []string {
	var out []string
	for name, of := range oldF {
		nf, exists := newF[name]
		if !exists {
			if of.required {
				out = append(out, fmt.Sprintf("FORWARD: required field %q was removed", name))
			}
			continue
		}
		if of.typ != nf.typ {
			out = append(out, fmt.Sprintf("FORWARD: field %q changed type from %s to %s", name, of.typ, nf.typ))
		}
	}
	return out
}

// CheckSchemaCompatibility evaluates candidate against lineage (ordered by
// ascending version) for the given mode and returns human-readable violations.
// Non-transitive modes compare only against the latest version. Definitions
// that are not parseable JSON are not semantically comparable and pass.
func CheckSchemaCompatibility(mode contracts.CompatibilityMode, candidateDefinition string, lineage []contracts.SchemaVersionRecord) []string {
	if mode == contracts.CompatibilityNone || len(lineage) == 0 {
		return nil
	}
	candidate, ok := parseSchemaFields(candidateDefinition)
	if !ok {
		return nil
	}

	var backward, forward, transitive bool
	switch mode {
	case contracts.CompatibilityBackward:
		backward = true
	case contracts.CompatibilityBackwardTransitive:
		backward, transitive = true, true
	case contracts.CompatibilityForward:
		forward = true
	case contracts.CompatibilityForwardTransitive:
		forward, transitive = true, true
	case contracts.CompatibilityFull:
		backward, forward = true, true
	default: // FULL_TRANSITIVE and anything unrecognised: strictest
		backward, forward, transitive = true, true, true
	}

	targets := lineage
	if !transitive {
		targets = lineage[len(lineage)-1:]
	}

	seen := map[string]bool{}
	var violations []string
	for _, past := range targets {
		prev, ok := parseSchemaFields(past.SchemaDefinition)
		if !ok {
			continue
		}
		var vs []string
		if backward {
			vs = append(vs, backwardViolations(prev, candidate)...)
		}
		if forward {
			vs = append(vs, forwardViolations(prev, candidate)...)
		}
		sort.Strings(vs)
		for _, v := range vs {
			msg := fmt.Sprintf("%s (vs version %d)", v, past.SchemaVersion)
			if !seen[msg] {
				seen[msg] = true
				violations = append(violations, msg)
			}
		}
	}
	return violations
}
