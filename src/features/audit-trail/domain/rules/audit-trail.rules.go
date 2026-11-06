package rules

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

var forbiddenSecretSubstrings = []string{
	"password",
	"passwd",
	"secret_key",
	"api_key",
	"private_key",
	"bearer ",
	"authorization:",
	"access_token",
	"refresh_token",
}

func ContainsForbiddenSecrets(raw string) bool {
	lower := strings.ToLower(raw)
	for _, sub := range forbiddenSecretSubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

func CanonicalizeJSONPayload(raw string) (string, error) {
	var obj any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return strings.TrimSpace(raw), nil
	}
	canonicalBytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(canonicalBytes), nil
}

func ComputeRecordHash(prevHash string, canonicalPayload string, signingSecret string) string {
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte(prevHash))
	mac.Write([]byte(canonicalPayload))
	return hex.EncodeToString(mac.Sum(nil))
}

func ComputeMerkleRoot(hashes []string) string {
	if len(hashes) == 0 {
		h := sha256.Sum256([]byte("EMPTY_SHARD_INTERVAL"))
		return hex.EncodeToString(h[:])
	}
	current := make([]string, len(hashes))
	copy(current, hashes)

	for len(current) > 1 {
		var nextLevel []string
		for i := 0; i < len(current); i += 2 {
			if i+1 < len(current) {
				combined := sha256.Sum256([]byte(current[i] + current[i+1]))
				nextLevel = append(nextLevel, hex.EncodeToString(combined[:]))
			} else {
				combined := sha256.Sum256([]byte(current[i] + current[i]))
				nextLevel = append(nextLevel, hex.EncodeToString(combined[:]))
			}
		}
		current = nextLevel
	}
	return current[0]
}

func SignCheckpointAnchor(rootHash string, headHash string, signingSecret string) string {
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte(rootHash))
	mac.Write([]byte(headHash))
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyCheckpointAnchor(rootHash string, headHash string, signingSecret string, expectedSig string) bool {
	computed := SignCheckpointAnchor(rootHash, headHash, signingSecret)
	return hmac.Equal([]byte(computed), []byte(expectedSig))
}
