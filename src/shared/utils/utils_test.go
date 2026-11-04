package utils_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/shared/utils"
)

func TestGenerateUUIDv7(t *testing.T) {
	val, err := utils.GenerateUUIDv7()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed, err := uuid.Parse(val)
	if err != nil {
		t.Fatalf("failed parsing generated uuid: %v", err)
	}
	if parsed.Version() != 7 {
		t.Errorf("expected UUID version 7, got %d", parsed.Version())
	}
}

func TestGeneratePublicID(t *testing.T) {
	id, err := utils.GeneratePublicID("otb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pattern := regexp.MustCompile(`^otb_[0-9a-fA-F]{26}$`)
	if !pattern.MatchString(id) {
		t.Errorf("public id '%s' does not match expected format", id)
	}
}

func TestNowUTCAndFormat(t *testing.T) {
	now := utils.NowUTC()
	if now.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", now.Location())
	}
	formatted := utils.FormatRFC3339UTC(now)
	if formatted[len(formatted)-1] != 'Z' {
		t.Errorf("expected RFC3339 formatted time to end in Z, got %s", formatted)
	}
}

