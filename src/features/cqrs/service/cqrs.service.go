package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/features/cqrs/domain/machines"
	"event-driven-platform/src/features/cqrs/domain/rules"
	"event-driven-platform/src/features/cqrs/infrastructure/ports"
)

var (
	ErrIdempotencyFingerprintMismatch = errors.New("idempotency key reused with mismatched payload fingerprint")
	ErrConcurrentExecutionInProgress  = errors.New("command execution currently in progress for this idempotency key")
	ErrExpectedVersionMismatch        = errors.New("aggregate current version does not match expected version")
)

type CQRSService struct {
	repo ports.CQRSRepositoryPort
}

func NewCQRSService(repo ports.CQRSRepositoryPort) *CQRSService {
	return &CQRSService{
		repo: repo,
	}
}

func (s *CQRSService) DispatchCommand(ctx context.Context, in contracts.DispatchCommandInput) (*contracts.DispatchCommandOutput, error) {
	norm, err := contracts.NormalizeDispatchCommandInput(in)
	if err != nil {
		return nil, err
	}

	fingerprint := rules.ComputeRequestFingerprint(norm.PayloadRaw)
	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()

	existing, err := s.repo.GetIdempotencyRecord(ctx, norm.TenantID, norm.IdempotencyKey)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		if existing.Status == contracts.CommandStatusInProgress {
			return nil, ErrConcurrentExecutionInProgress
		}

		outcome, matched := machines.DetermineReplayOutcome(*existing, fingerprint)
		if !matched {
			return nil, ErrIdempotencyFingerprintMismatch
		}

		return &contracts.DispatchCommandOutput{
			CommandID:        existing.PublicID,
			AggregateID:      existing.AggregateID,
			NewVersion:       existing.Version,
			Status:           outcome,
			ConsistencyToken: existing.ConsistencyToken,
			ResponsePayload:  existing.ResponsePayload,
		}, nil
	}

	cmdPubID := fmt.Sprintf("cmd_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])
	cmdRecord := contracts.CommandRecord{
		ID:                 uuid.New(),
		PublicID:           cmdPubID,
		TenantID:           norm.TenantID,
		SourceService:      norm.SourceService,
		ExternalID:         norm.ExternalID,
		CorrelationID:      corrUUID,
		IdempotencyKey:     norm.IdempotencyKey,
		CommandName:        norm.CommandName,
		AggregateType:      norm.AggregateType,
		AggregateID:        norm.AggregateID,
		RequestFingerprint: fingerprint,
		Status:             contracts.CommandStatusInProgress,
		ResponsePayload:    "{}",
		ConsistencyToken:   "",
		Version:            1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	if err := s.repo.CreateIdempotencyRecord(ctx, cmdRecord); err != nil {
		return nil, err
	}

	agg, err := s.repo.GetAggregateForUpdate(ctx, norm.TenantID, norm.AggregateType, norm.AggregateID)
	if err != nil {
		return nil, err
	}

	var currentVersion int64 = 0
	if agg != nil {
		currentVersion = agg.CurrentVersion
	}

	if norm.ExpectedVersion > 0 && currentVersion != norm.ExpectedVersion {
		conflictPayload := fmt.Sprintf(`{"error":"CONFLICT","current_version":%d,"expected_version":%d}`, currentVersion, norm.ExpectedVersion)
		_ = s.repo.UpdateIdempotencyRecord(ctx, cmdRecord.ID, norm.TenantID, contracts.CommandStatusConflict, conflictPayload, "")
		return &contracts.DispatchCommandOutput{
			CommandID:       cmdPubID,
			AggregateID:     norm.AggregateID,
			NewVersion:      currentVersion,
			Status:          contracts.CommandStatusConflict,
			ResponsePayload: conflictPayload,
		}, ErrExpectedVersionMismatch
	}

	newVersion := currentVersion + 1
	aggPubID := fmt.Sprintf("agg_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])
	aggRecord := contracts.AggregateRecord{
		ID:             uuid.New(),
		PublicID:       aggPubID,
		TenantID:       norm.TenantID,
		SourceService:  norm.SourceService,
		ExternalID:     uuid.New().String(),
		CorrelationID:  corrUUID,
		AggregateType:  norm.AggregateType,
		AggregateID:    norm.AggregateID,
		CurrentVersion: newVersion,
		StatePayload:   norm.PayloadRaw,
		IsDeleted:      false,
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.repo.UpsertAggregateMutation(ctx, aggRecord); err != nil {
		return nil, err
	}

	signingKey, err := s.repo.GetActiveSigningKey(ctx, norm.TenantID)
	if err != nil {
		return nil, err
	}

	var secretValue = "default_system_cqrs_secret_key_32"
	var keyVersion = 1
	if signingKey != nil {
		secretValue = signingKey.RawSecretValue
		keyVersion = signingKey.KeyVersion
	}

	tokenObj := contracts.ConsistencyToken{
		TenantID:      norm.TenantID,
		PrincipalID:   norm.PrincipalID,
		AggregateType: norm.AggregateType,
		AggregateID:   norm.AggregateID,
		MinVersion:    newVersion,
		ExpiresAt:     now.Add(15 * time.Minute),
		KeyVersion:    keyVersion,
	}
	tokenStr, _ := rules.MintConsistencyToken(tokenObj, secretValue)

	responsePayload := fmt.Sprintf(`{"aggregate_id":"%s","version":%d,"status":"COMPLETED"}`, norm.AggregateID, newVersion)
	if err := s.repo.UpdateIdempotencyRecord(ctx, cmdRecord.ID, norm.TenantID, contracts.CommandStatusCompleted, responsePayload, tokenStr); err != nil {
		return nil, err
	}

	return &contracts.DispatchCommandOutput{
		CommandID:        cmdPubID,
		AggregateID:      norm.AggregateID,
		NewVersion:       newVersion,
		Status:           contracts.CommandStatusCompleted,
		ConsistencyToken: tokenStr,
		ResponsePayload:  responsePayload,
	}, nil
}

func (s *CQRSService) ResolveQuery(ctx context.Context, in contracts.ResolveQueryInput) (*contracts.ResolveQueryOutput, error) {
	norm, err := contracts.NormalizeResolveQueryInput(in)
	if err != nil {
		return nil, err
	}

	if norm.ConsistencyTokenRaw == "" {
		return &contracts.ResolveQueryOutput{
			Data:              fmt.Sprintf(`{"view_name":"%s","aggregate_id":"%s"}`, norm.ViewName, norm.AggregateID),
			Version:           1,
			ConsistencyStatus: contracts.ResolutionImmediate,
			ExpectedVersion:   1,
		}, nil
	}

	signingKey, _ := s.repo.GetActiveSigningKey(ctx, norm.TenantID)
	secretValue := "default_system_cqrs_secret_key_32"
	if signingKey != nil {
		secretValue = signingKey.RawSecretValue
	}

	token, valid := rules.VerifyConsistencyToken(norm.ConsistencyTokenRaw, secretValue, time.Now().UTC())
	if !valid {
		return &contracts.ResolveQueryOutput{
			Data:              fmt.Sprintf(`{"view_name":"%s","aggregate_id":"%s"}`, norm.ViewName, norm.AggregateID),
			Version:           1,
			ConsistencyStatus: contracts.ResolutionImmediate,
			ExpectedVersion:   1,
		}, nil
	}

	var simulatedViewVersion int64 = 1
	resolution := rules.EvaluateConsistencyResolution(simulatedViewVersion, token.MinVersion)

	return &contracts.ResolveQueryOutput{
		Data:              fmt.Sprintf(`{"view_name":"%s","aggregate_id":"%s","version":%d}`, norm.ViewName, norm.AggregateID, simulatedViewVersion),
		Version:           simulatedViewVersion,
		ConsistencyStatus: resolution,
		ExpectedVersion:   token.MinVersion,
	}, nil
}

func (s *CQRSService) RegisterSigningKey(ctx context.Context, in contracts.RegisterSigningKeyInput) (*contracts.TokenSigningKeyRecord, error) {
	norm, err := contracts.NormalizeRegisterSigningKeyInput(in)
	if err != nil {
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("ctk_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	keyHash := sha256.Sum256([]byte(norm.RawSecretValue))

	record := contracts.TokenSigningKeyRecord{
		ID:             uuid.New(),
		PublicID:       pubID,
		TenantID:       norm.TenantID,
		SourceService:  norm.SourceService,
		ExternalID:     uuid.New().String(),
		CorrelationID:  corrUUID,
		KeyVersion:     norm.KeyVersion,
		SecretKeyHash:  hex.EncodeToString(keyHash[:]),
		RawSecretValue: norm.RawSecretValue,
		Status:         "ACTIVE",
		ExpiresAt:      norm.ExpiresAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.repo.RegisterSigningKey(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *CQRSService) AuthoritativeRead(ctx context.Context, in contracts.AuthoritativeReadInput) (*contracts.AggregateRecord, error) {
	norm, err := contracts.NormalizeAuthoritativeReadInput(in)
	if err != nil {
		return nil, err
	}

	return s.repo.GetAggregateForUpdate(ctx, norm.TenantID, norm.AggregateType, norm.AggregateID)
}
