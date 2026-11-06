package rules

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"event-driven-platform/src/features/cqrs/contracts"
)

func ComputeRequestFingerprint(payload string) string {
	h := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(h[:])
}

func ValidateExpectedVersion(loadedVersion int64, expectedVersion int64) bool {
	return loadedVersion == expectedVersion
}

func MintConsistencyToken(t contracts.ConsistencyToken, secret string) (string, error) {
	mac := hmac.New(sha256.New, []byte(secret))
	signingString := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d",
		t.TenantID,
		t.PrincipalID,
		t.AggregateType,
		t.AggregateID,
		t.MinVersion,
		t.ExpiresAt.Unix(),
		t.KeyVersion,
	)
	mac.Write([]byte(signingString))
	t.Signature = hex.EncodeToString(mac.Sum(nil))

	bytes, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(bytes)
	return fmt.Sprintf("c_tok_%s", encoded), nil
}

func VerifyConsistencyToken(rawToken string, secret string, now time.Time) (*contracts.ConsistencyToken, bool) {
	if !strings.HasPrefix(rawToken, "c_tok_") {
		return nil, false
	}
	encoded := strings.TrimPrefix(rawToken, "c_tok_")
	bytes, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, false
	}

	var token contracts.ConsistencyToken
	if err := json.Unmarshal(bytes, &token); err != nil {
		return nil, false
	}

	if now.After(token.ExpiresAt) {
		return nil, false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	signingString := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d",
		token.TenantID,
		token.PrincipalID,
		token.AggregateType,
		token.AggregateID,
		token.MinVersion,
		token.ExpiresAt.Unix(),
		token.KeyVersion,
	)
	mac.Write([]byte(signingString))
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(token.Signature), []byte(expectedSig)) {
		return nil, false
	}

	return &token, true
}

func EvaluateConsistencyResolution(viewVersion int64, minVersion int64) contracts.ConsistencyResolution {
	if viewVersion >= minVersion {
		return contracts.ResolutionImmediate
	}
	return contracts.ResolutionPending
}
