package rules

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"event-driven-platform/src/features/encryption/contracts"
)

var (
	ErrTagMismatch = errors.New("aead authentication tag mismatch")
)

func BuildAAD(ctx contracts.AADContext) []byte {
	formatted := fmt.Sprintf("%s|%s|%s|%s|%d|%s",
		strings.TrimSpace(ctx.TenantID),
		strings.TrimSpace(ctx.SubjectID),
		strings.TrimSpace(ctx.AggregateID),
		strings.TrimSpace(ctx.FieldPath),
		ctx.SchemaVersion,
		strings.TrimSpace(ctx.KeyRef),
	)
	return []byte(formatted)
}

func GenerateNonce() ([]byte, error) {
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

func EncryptAESGCM(key []byte, nonce []byte, plaintext []byte, aad []byte) (string, string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}

	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	tagSize := gcm.Overhead()
	if len(sealed) < tagSize {
		return "", "", errors.New("sealed ciphertext shorter than tag size")
	}

	cipherBytes := sealed[:len(sealed)-tagSize]
	tagBytes := sealed[len(sealed)-tagSize:]

	return hex.EncodeToString(cipherBytes), hex.EncodeToString(tagBytes), nil
}

func DecryptAESGCM(key []byte, nonce []byte, cipherHex string, tagHex string, aad []byte) ([]byte, error) {
	cipherBytes, err := hex.DecodeString(cipherHex)
	if err != nil {
		return nil, err
	}
	tagBytes, err := hex.DecodeString(tagHex)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	fullSealed := append(cipherBytes, tagBytes...)
	plaintext, err := gcm.Open(nil, nonce, fullSealed, aad)
	if err != nil {
		return nil, ErrTagMismatch
	}

	return plaintext, nil
}

func ComputeBlindIndex(tenantID string, fieldClass string, rawValue string, secretKey []byte) string {
	normalized := strings.ToLower(strings.TrimSpace(rawValue))
	mac := hmac.New(sha256.New, secretKey)
	mac.Write([]byte(tenantID))
	mac.Write([]byte(":"))
	mac.Write([]byte(fieldClass))
	mac.Write([]byte(":"))
	mac.Write([]byte(normalized))
	return "bix_" + hex.EncodeToString(mac.Sum(nil))[:32]
}
