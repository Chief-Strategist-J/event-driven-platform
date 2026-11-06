package adapters

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErasedMarker = []byte("[ERASED]")
)

type InMemoryCryptoAdapter struct {
	mu   sync.RWMutex
	keys map[string]*contracts.DEKRecord
}

func NewInMemoryCryptoAdapter() *InMemoryCryptoAdapter {
	return &InMemoryCryptoAdapter{
		keys: make(map[string]*contracts.DEKRecord),
	}
}

func (a *InMemoryCryptoAdapter) makeKey(tenantID string, subjectID string, periodKey string) string {
	return fmt.Sprintf("%s:%s:%s", tenantID, subjectID, periodKey)
}

func (a *InMemoryCryptoAdapter) StoreDEK(_ context.Context, dek contracts.DEKRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	k := a.makeKey(dek.TenantID, dek.SubjectID, dek.PeriodKey)
	a.keys[k] = &dek
	return nil
}

func (a *InMemoryCryptoAdapter) GetDEK(_ context.Context, tenantID string, subjectID string, periodKey string) (*contracts.DEKRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	k := a.makeKey(tenantID, subjectID, periodKey)
	rec, exists := a.keys[k]
	if !exists {
		return nil, errors.NewNotFoundError(fmt.Sprintf("dek not found for %s", k))
	}
	return rec, nil
}

func (a *InMemoryCryptoAdapter) ShredDEK(_ context.Context, tenantID string, subjectID string, periodKey string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	k := a.makeKey(tenantID, subjectID, periodKey)
	rec, exists := a.keys[k]
	if !exists {
		return errors.NewNotFoundError(fmt.Sprintf("dek not found for %s", k))
	}

	now := time.Now().UTC()
	rec.Status = contracts.KeyStatusShredded
	rec.ShreddedAt = &now
	rec.WrappedDEK = []byte{}
	return nil
}

func (a *InMemoryCryptoAdapter) deriveKey(raw []byte) []byte {
	h := sha256.Sum256(raw)
	return h[:]
}

func (a *InMemoryCryptoAdapter) EncryptField(ctx context.Context, tenantID string, subjectID string, periodKey string, plaintext []byte) ([]byte, error) {
	dek, err := a.GetDEK(ctx, tenantID, subjectID, periodKey)
	if err != nil {
		return nil, err
	}
	if dek.Status == contracts.KeyStatusShredded {
		return nil, errors.NewConflictError("cannot encrypt with shredded DEK")
	}

	key := a.deriveKey(dek.WrappedDEK)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("cipher initialization error: %v", err))
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("gcm error: %v", err))
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("nonce error: %v", err))
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

func (a *InMemoryCryptoAdapter) DecryptField(ctx context.Context, tenantID string, subjectID string, periodKey string, ciphertext []byte) ([]byte, error) {
	dek, err := a.GetDEK(ctx, tenantID, subjectID, periodKey)
	if err != nil {
		return nil, err
	}
	if dek.Status == contracts.KeyStatusShredded {
		return ErasedMarker, nil
	}

	key := a.deriveKey(dek.WrappedDEK)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("cipher initialization error: %v", err))
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("gcm error: %v", err))
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.NewValidationError("ciphertext too short")
	}

	nonce, encryptedData := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("decrypt error: %v", err))
	}

	return plaintext, nil
}
