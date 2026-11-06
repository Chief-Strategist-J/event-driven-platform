package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/domain/machines"
	"event-driven-platform/src/features/encryption/domain/rules"
	"event-driven-platform/src/features/encryption/infrastructure/ports"
)

type EncryptionService struct {
	repo       ports.EncryptionRepositoryPort
	masterKEK  []byte
	dekCache   sync.Map
	bixSecret  []byte
	tracer     trace.Tracer
}

func NewEncryptionService(repo ports.EncryptionRepositoryPort, masterSecret ...string) *EncryptionService {
	sec := "encryption-master-secret-at-least-32-chars-long-for-kek"
	if len(masterSecret) > 0 && len(masterSecret[0]) >= 32 {
		sec = masterSecret[0]
	}
	hash := sha256.Sum256([]byte(sec))
	bixHash := sha256.Sum256([]byte("bix:" + sec))

	return &EncryptionService{
		repo:      repo,
		masterKEK: hash[:],
		bixSecret: bixHash[:],
		tracer:    otel.Tracer("EncryptionService"),
	}
}

func (s *EncryptionService) EnsureKEK(ctx context.Context, tenantID string) (*contracts.KEKRecord, error) {
	kek, err := s.repo.GetActiveKEK(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if kek != nil {
		return kek, nil
	}

	rawID := uuid.New().String()
	pubID := fmt.Sprintf("kek_%s", strings.ReplaceAll(rawID, "-", "")[:20])
	newKEK := contracts.KEKRecord{
		ID:          rawID,
		PublicID:    pubID,
		TenantID:    tenantID,
		KeyName:     fmt.Sprintf("%s-root-kek", tenantID),
		KeyVersion:  1,
		KMSProvider: "LOCAL_SIMULATED",
		KMSKeyARN:   fmt.Sprintf("arn:simulated:kms:%s:key/%s", tenantID, pubID),
		State:       "ACTIVE",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	if err := s.repo.RegisterKEK(ctx, newKEK); err != nil {
		return nil, err
	}
	return &newKEK, nil
}

func (s *EncryptionService) EnsureDEK(ctx context.Context, tenantID string, subjectID string, kekPublicID string) (*contracts.DEKRecord, []byte, error) {
	cacheKey := fmt.Sprintf("%s:%s", tenantID, subjectID)
	if val, ok := s.dekCache.Load(cacheKey); ok {
		if pair, ok := val.(cachedDEKPair); ok && pair.dek.State == "ACTIVE" {
			return pair.dek, pair.rawKey, nil
		}
	}

	dek, err := s.repo.GetDEKBySubject(ctx, tenantID, subjectID)
	if err != nil {
		return nil, nil, err
	}
	if dek != nil {
		if dek.State == "SHREDDED" {
			return dek, nil, nil
		}
		rawDEK, err := s.unwrapDEK(dek.EncryptedDEKBlob)
		if err != nil {
			return nil, nil, err
		}
		s.dekCache.Store(cacheKey, cachedDEKPair{dek: dek, rawKey: rawDEK})
		return dek, rawDEK, nil
	}

	rawDEK := make([]byte, 32)
	if _, err := rand.Read(rawDEK); err != nil {
		return nil, nil, err
	}

	wrappedHex, err := s.wrapDEK(rawDEK)
	if err != nil {
		return nil, nil, err
	}

	rawID := uuid.New().String()
	pubID := fmt.Sprintf("dek_%s", strings.ReplaceAll(rawID, "-", "")[:20])
	newDEK := contracts.DEKRecord{
		ID:               rawID,
		PublicID:         pubID,
		TenantID:         tenantID,
		SubjectID:        subjectID,
		KEKPublicID:      kekPublicID,
		EncryptedDEKBlob: wrappedHex,
		Algorithm:        "AES-256-GCM",
		State:            "ACTIVE",
		CreatedAt:        time.Now().UTC(),
		RotatedAt:        time.Now().UTC(),
		ShreddedAt:       time.Unix(0, 0).UTC(),
	}

	if err := s.repo.RegisterDEK(ctx, newDEK); err != nil {
		return nil, nil, err
	}

	s.dekCache.Store(cacheKey, cachedDEKPair{dek: &newDEK, rawKey: rawDEK})
	return &newDEK, rawDEK, nil
}

func (s *EncryptionService) EncryptField(ctx context.Context, in contracts.EncryptFieldInput) (*contracts.EncryptedValue, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.EncryptField")
	defer span.End()

	norm, err := contracts.NormalizeEncryptFieldInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	kek, err := s.EnsureKEK(ctx, norm.TenantID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	dek, rawDEK, err := s.EnsureDEK(ctx, norm.TenantID, norm.SubjectID, kek.PublicID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if dek.State == "SHREDDED" {
		err := errors.New("cannot encrypt: subject DEK has been shredded")
		span.RecordError(err)
		return nil, err
	}

	nonce, err := rules.GenerateNonce()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	aadCtx := contracts.AADContext{
		TenantID:      norm.TenantID,
		SubjectID:     norm.SubjectID,
		AggregateID:   norm.AggregateID,
		FieldPath:     norm.FieldPath,
		SchemaVersion: norm.SchemaVersion,
		KeyRef:        dek.PublicID,
	}
	aadBytes := rules.BuildAAD(aadCtx)

	cipherHex, tagHex, err := rules.EncryptAESGCM(rawDEK, nonce, []byte(norm.Plaintext), aadBytes)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &contracts.EncryptedValue{
		SchemeVersion: "v1",
		KeyRef:        dek.PublicID,
		Nonce:         hex.EncodeToString(nonce),
		Ciphertext:    cipherHex,
		Tag:           tagHex,
	}, nil
}

func (s *EncryptionService) DecryptField(ctx context.Context, in contracts.DecryptFieldInput) (*contracts.DecryptFieldOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.DecryptField")
	defer span.End()

	norm, err := contracts.NormalizeDecryptFieldInput(in)
	if err != nil {
		span.RecordError(err)
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeMalformed,
			Message: err.Error(),
		}, nil
	}

	isEntitled := norm.ConsumerRole != "UNAUTHORIZED" && norm.ConsumerRole != "FORBIDDEN"
	if !isEntitled {
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeUnauthorized,
			Message: "consumer lacks entitlement for encrypted field",
		}, nil
	}

	dek, err := s.repo.GetDEKByPublicID(ctx, norm.TenantID, norm.EncryptedValue.KeyRef)
	if err != nil {
		span.RecordError(err)
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeUnavailable,
			Message: "key repository unavailable",
		}, nil
	}
	if dek == nil || dek.State == "SHREDDED" {
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeErased,
			Message: "subject crypto-shredded",
		}, nil
	}

	rawDEK, err := s.unwrapDEK(dek.EncryptedDEKBlob)
	if err != nil {
		span.RecordError(err)
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeUnavailable,
			Message: "failed to unwrap DEK",
		}, nil
	}

	nonceBytes, err := hex.DecodeString(norm.EncryptedValue.Nonce)
	if err != nil {
		return &contracts.DecryptFieldOutput{
			Outcome: contracts.OutcomeMalformed,
			Message: "malformed nonce hex",
		}, nil
	}

	aadCtx := contracts.AADContext{
		TenantID:      norm.TenantID,
		SubjectID:     norm.SubjectID,
		AggregateID:   norm.AggregateID,
		FieldPath:     norm.FieldPath,
		SchemaVersion: norm.SchemaVersion,
		KeyRef:        norm.EncryptedValue.KeyRef,
	}
	aadBytes := rules.BuildAAD(aadCtx)

	plaintext, decryptErr := rules.DecryptAESGCM(rawDEK, nonceBytes, norm.EncryptedValue.Ciphertext, norm.EncryptedValue.Tag, aadBytes)
	outcome := machines.EvaluateDecryptOutcome(dek.State, isEntitled, decryptErr)

	if outcome != contracts.OutcomeDecrypted {
		msg := "decryption verification failed"
		if decryptErr != nil {
			msg = decryptErr.Error()
		}
		return &contracts.DecryptFieldOutput{
			Outcome: outcome,
			Message: msg,
		}, nil
	}

	return &contracts.DecryptFieldOutput{
		Outcome:   contracts.OutcomeDecrypted,
		Plaintext: string(plaintext),
		Message:   "decryption successful",
	}, nil
}

func (s *EncryptionService) ShredSubject(ctx context.Context, in contracts.ShredSubjectInput) error {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.ShredSubject")
	defer span.End()

	norm, err := contracts.NormalizeShredSubjectInput(in)
	if err != nil {
		span.RecordError(err)
		return err
	}

	dek, err := s.repo.GetDEKBySubject(ctx, norm.TenantID, norm.SubjectID)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if dek == nil {
		return errors.New("no DEK found for subject to shred")
	}

	if err := s.repo.ShredSubjectDEK(ctx, norm, dek.PublicID); err != nil {
		span.RecordError(err)
		return err
	}

	cacheKey := fmt.Sprintf("%s:%s", norm.TenantID, norm.SubjectID)
	s.dekCache.Delete(cacheKey)

	return nil
}

func (s *EncryptionService) GenerateBlindIndex(ctx context.Context, in contracts.BlindIndexInput) (*contracts.BlindIndexOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.GenerateBlindIndex")
	defer span.End()

	norm, err := contracts.NormalizeBlindIndexInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	hash := rules.ComputeBlindIndex(norm.TenantID, norm.FieldClass, norm.RawValue, s.bixSecret)
	return &contracts.BlindIndexOutput{
		BlindIndexHash: hash,
	}, nil
}

func (s *EncryptionService) ProvisionKEK(ctx context.Context, in *contracts.CreateKEKInput) (*contracts.KEKRecord, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.ProvisionKEK")
	defer span.End()

	rawID := uuid.New().String()
	pubID := fmt.Sprintf("kek_%s", strings.ReplaceAll(rawID, "-", "")[:20])
	newKEK := contracts.KEKRecord{
		ID:          rawID,
		PublicID:    pubID,
		TenantID:    in.TenantID,
		KeyName:     in.KeyName,
		KeyVersion:  1,
		KMSProvider: in.KMSProvider,
		KMSKeyARN:   in.KMSKeyARN,
		State:       "ACTIVE",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	if err := s.repo.RegisterKEK(ctx, newKEK); err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &newKEK, nil
}

func (s *EncryptionService) EncryptPayload(ctx context.Context, in *contracts.EncryptPayloadInput) (*contracts.EncryptPayloadOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.EncryptPayload")
	defer span.End()

	var payloadMap map[string]any
	if err := json.Unmarshal(in.PlaintextJSON, &payloadMap); err != nil {
		span.RecordError(err)
		return nil, err
	}

	kek, err := s.EnsureKEK(ctx, in.TenantID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	dek, _, err := s.EnsureDEK(ctx, in.TenantID, in.SubjectID, kek.PublicID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	for _, field := range in.SensitiveFields {
		val, exists := payloadMap[field]
		if !exists || val == nil {
			continue
		}
		valStr := fmt.Sprintf("%v", val)
		encVal, err := s.EncryptField(ctx, contracts.EncryptFieldInput{
			TenantID:      in.TenantID,
			SubjectID:     in.SubjectID,
			AggregateID:   fmt.Sprintf("agg_%s", in.SubjectID),
			FieldPath:     field,
			SchemaVersion: 1,
			Plaintext:     valStr,
		})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		encBytes, _ := json.Marshal(encVal)
		payloadMap[field] = string(encBytes)
	}

	modifiedBytes, err := json.Marshal(payloadMap)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	headers := map[string]string{
		"X-Encrypted-Fields": strings.Join(in.SensitiveFields, ","),
		"X-DEK-Public-ID":    dek.PublicID,
	}

	return &contracts.EncryptPayloadOutput{
		DEKPublicID:   dek.PublicID,
		EncryptedJSON: modifiedBytes,
		Headers:       headers,
	}, nil
}

func (s *EncryptionService) DecryptPayload(ctx context.Context, in *contracts.DecryptPayloadInput) (*contracts.DecryptPayloadOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.DecryptPayload")
	defer span.End()

	dek, err := s.repo.GetDEKBySubject(ctx, in.TenantID, in.SubjectID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var payloadMap map[string]any
	if err := json.Unmarshal(in.EncryptedJSON, &payloadMap); err != nil {
		span.RecordError(err)
		return nil, err
	}

	fieldsHeader := in.Headers["X-Encrypted-Fields"]
	fields := strings.Split(fieldsHeader, ",")

	if dek != nil && dek.State == "SHREDDED" {
		for _, f := range fields {
			field := strings.TrimSpace(f)
			if field != "" {
				payloadMap[field] = nil
			}
		}
		shreddedBytes, _ := json.Marshal(payloadMap)
		return &contracts.DecryptPayloadOutput{
			DecryptedJSON:  shreddedBytes,
			IsShredded:     true,
			ShreddedReason: "GDPR_RTBF",
		}, nil
	}

	for _, f := range fields {
		field := strings.TrimSpace(f)
		if field == "" {
			continue
		}
		encStr, ok := payloadMap[field].(string)
		if !ok || encStr == "" {
			continue
		}
		var encVal contracts.EncryptedValue
		if err := json.Unmarshal([]byte(encStr), &encVal); err != nil {
			continue
		}
		out, err := s.DecryptField(ctx, contracts.DecryptFieldInput{
			TenantID:       in.TenantID,
			SubjectID:      in.SubjectID,
			AggregateID:    fmt.Sprintf("agg_%s", in.SubjectID),
			FieldPath:      field,
			SchemaVersion:  1,
			ConsumerRole:   in.AuthorizedRole,
			EncryptedValue: encVal,
		})
		if err != nil || out.Outcome != contracts.OutcomeDecrypted {
			continue
		}
		payloadMap[field] = out.Plaintext
	}

	decryptedBytes, err := json.Marshal(payloadMap)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &contracts.DecryptPayloadOutput{
		DecryptedJSON:  decryptedBytes,
		IsShredded:     false,
		ShreddedReason: "",
	}, nil
}

func (s *EncryptionService) CryptoShredSubject(ctx context.Context, in *contracts.ShredSubjectInput) (*contracts.CryptoShreddingRecord, error) {
	ctx, span := s.tracer.Start(ctx, "EncryptionService.CryptoShredSubject")
	defer span.End()

	dek, err := s.repo.GetDEKBySubject(ctx, in.TenantID, in.SubjectID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if dek == nil {
		return nil, errors.New("no DEK found for subject")
	}

	if err := s.ShredSubject(ctx, *in); err != nil {
		span.RecordError(err)
		return nil, err
	}

	auditID := uuid.New().String()
	pubID := fmt.Sprintf("csa_%s", strings.ReplaceAll(auditID, "-", "")[:20])

	return &contracts.CryptoShreddingRecord{
		ID:          auditID,
		PublicID:    pubID,
		TenantID:    in.TenantID,
		SubjectID:   in.SubjectID,
		DEKPublicID: dek.PublicID,
		Reason:      in.Reason,
		RequestedBy: in.RequestedBy,
		ShreddedAt:  time.Now().UTC(),
	}, nil
}

func (s *EncryptionService) wrapDEK(rawDEK []byte) (string, error) {
	nonce, err := rules.GenerateNonce()
	if err != nil {
		return "", err
	}
	aad := []byte("DEK_WRAP_KEK_V1")
	cHex, tHex, err := rules.EncryptAESGCM(s.masterKEK, nonce, rawDEK, aad)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s:%s", hex.EncodeToString(nonce), cHex, tHex), nil
}

func (s *EncryptionService) unwrapDEK(wrapped string) ([]byte, error) {
	parts := strings.Split(wrapped, ":")
	if len(parts) != 3 {
		return nil, errors.New("malformed wrapped DEK blob")
	}
	nonceBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}
	aad := []byte("DEK_WRAP_KEK_V1")
	return rules.DecryptAESGCM(s.masterKEK, nonceBytes, parts[1], parts[2], aad)
}

type cachedDEKPair struct {
	dek    *contracts.DEKRecord
	rawKey []byte
}
