package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

func GenerateUUIDv7() (string, error) {
	val, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed generating uuidv7: %w", err)
	}
	return val.String(), nil
}

func GeneratePublicID(prefix string) (string, error) {
	bytes := make([]byte, 13)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed generating random bytes: %w", err)
	}
	randomStr := hex.EncodeToString(bytes)
	return fmt.Sprintf("%s_%s", prefix, randomStr), nil
}

