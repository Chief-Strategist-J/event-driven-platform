package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/audit-trail/contracts"
	"event-driven-platform/src/features/audit-trail/domain/machines"
	"event-driven-platform/src/features/audit-trail/domain/rules"
	"event-driven-platform/src/features/audit-trail/infrastructure/ports"
)

const (
	GenesisPrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
)

type AuditTrailService struct {
	repo          ports.AuditTrailRepositoryPort
	signingSecret string
	tracer        trace.Tracer
}

func NewAuditTrailService(repo ports.AuditTrailRepositoryPort, optionalSecret ...string) *AuditTrailService {
	signingSecret := "audit-trail-default-signing-secret-at-least-32-chars-long"
	if len(optionalSecret) > 0 && len(optionalSecret[0]) >= 32 {
		signingSecret = optionalSecret[0]
	}
	return &AuditTrailService{
		repo:          repo,
		signingSecret: signingSecret,
		tracer:        otel.Tracer("AuditTrailService"),
	}
}

func (s *AuditTrailService) EmitAuditRecord(ctx context.Context, in contracts.EmitAuditInput) (*contracts.AuditRecord, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.EmitAuditRecord")
	defer span.End()

	payload := in.RawPayload
	if payload == "" {
		payload = in.CanonicalPayload
	}

	ingestIn := contracts.IngestAuditRecordInput{
		TenantID:        in.TenantID,
		SourceService:   in.SourceService,
		ExternalID:      in.ExternalID,
		CorrelationID:   in.CorrelationID,
		ShardID:         in.ShardID,
		ActorID:         in.ActorID,
		ActorType:       in.ActorType,
		ActionName:      in.ActionName,
		TargetType:      in.TargetType,
		TargetID:        in.TargetID,
		Outcome:         in.Outcome,
		ComplianceClass: in.ComplianceClass,
		RawPayload:      payload,
		OccurredAt:      in.OccurredAt,
	}

	return s.IngestAuditRecord(ctx, ingestIn)
}

func (s *AuditTrailService) VerifyChain(ctx context.Context, in contracts.VerifyChainInput) (*contracts.VerifyChainOutput, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.VerifyChain")
	defer span.End()

	verifyIn := contracts.VerifyShardIntegrityInput{
		TenantID:      in.TenantID,
		ShardID:       in.ShardID,
		SigningSecret: in.SigningSecret,
	}

	latestRec, err := s.repo.GetLatestRecord(ctx, in.TenantID, in.ShardID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if latestRec == nil {
		return &contracts.VerifyChainOutput{
			IsIntact:        true,
			RecordsVerified: 0,
			LatestSequence:  0,
		}, nil
	}

	records, err := s.repo.GetShardRecordsRange(ctx, verifyIn, 1, latestRec.SequenceNumber)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	currentPrevHash := GenesisPrevHash
	for _, rec := range records {
		expectedHash := rules.ComputeRecordHash(currentPrevHash, rec.CanonicalPayload, in.SigningSecret)
		if rec.RecordHash != expectedHash {
			return &contracts.VerifyChainOutput{
				IsIntact:         false,
				RecordsVerified:  int64(len(records)),
				LatestSequence:   rec.SequenceNumber,
				LatestRecordHash: rec.RecordHash,
			}, nil
		}
		currentPrevHash = rec.RecordHash
	}

	return &contracts.VerifyChainOutput{
		IsIntact:         true,
		RecordsVerified:  int64(len(records)),
		LatestSequence:   latestRec.SequenceNumber,
		LatestRecordHash: latestRec.RecordHash,
	}, nil
}

func (s *AuditTrailService) IngestAuditRecord(ctx context.Context, in contracts.IngestAuditRecordInput) (*contracts.AuditRecord, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.IngestAuditRecord")
	defer span.End()

	norm, err := contracts.NormalizeIngestAuditRecordInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if rules.ContainsForbiddenSecrets(norm.RawPayload) {
		err := contracts.ErrSecretDetected
		span.RecordError(err)
		return nil, err
	}

	canonicalPayload, err := rules.CanonicalizeJSONPayload(norm.RawPayload)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	latest, err := s.repo.GetLatestRecord(ctx, norm.TenantID, norm.ShardID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	prevHash := GenesisPrevHash
	var seqNum int64 = 1
	if latest != nil {
		prevHash = latest.RecordHash
		seqNum = latest.SequenceNumber + 1
	}

	recordHash := rules.ComputeRecordHash(prevHash, canonicalPayload, s.signingSecret)
	recID := uuid.New().String()
	pubID := fmt.Sprintf("aud_%s", strings.ReplaceAll(recID, "-", "")[:24])

	rec := contracts.AuditRecord{
		ID:               recID,
		PublicID:         pubID,
		TenantID:         norm.TenantID,
		SourceService:    norm.SourceService,
		ExternalID:       norm.ExternalID,
		CorrelationID:    norm.CorrelationID,
		ShardID:          norm.ShardID,
		SequenceNumber:   seqNum,
		ActorID:          norm.ActorID,
		ActorType:        norm.ActorType,
		ActionName:       norm.ActionName,
		TargetType:       norm.TargetType,
		TargetID:         norm.TargetID,
		Outcome:          norm.Outcome,
		ComplianceClass:  norm.ComplianceClass,
		PrevRecordHash:   prevHash,
		RecordHash:       recordHash,
		CanonicalPayload: canonicalPayload,
		OccurredAt:       norm.OccurredAt,
		IngestedAt:       time.Now().UTC(),
	}

	if err := s.repo.AppendAuditRecord(ctx, rec); err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &rec, nil
}

func (s *AuditTrailService) CreateCheckpoint(ctx context.Context, in contracts.CreateCheckpointInput) (*contracts.CheckpointRecord, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.CreateCheckpoint")
	defer span.End()

	norm, err := contracts.NormalizeCreateCheckpointInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	latestRec, err := s.repo.GetLatestRecord(ctx, norm.TenantID, norm.ShardID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if latestRec == nil {
		return nil, fmt.Errorf("no audit records found in shard %s to checkpoint", norm.ShardID)
	}

	latestCP, err := s.repo.GetLatestCheckpoint(ctx, norm.TenantID, norm.ShardID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var startSeq int64 = 1
	var cpSeq int64 = 1
	if latestCP != nil {
		startSeq = latestCP.EndSequence + 1
		cpSeq = latestCP.CheckpointSeq + 1
	}

	if startSeq > latestRec.SequenceNumber {
		return latestCP, nil
	}

	verifyIn := contracts.VerifyShardIntegrityInput{
		TenantID:      norm.TenantID,
		ShardID:       norm.ShardID,
		SigningSecret: norm.SigningSecret,
	}
	records, err := s.repo.GetShardRecordsRange(ctx, verifyIn, startSeq, latestRec.SequenceNumber)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var hashes []string
	for _, r := range records {
		hashes = append(hashes, r.RecordHash)
	}

	merkleRoot := rules.ComputeMerkleRoot(hashes)
	sig := rules.SignCheckpointAnchor(merkleRoot, latestRec.RecordHash, norm.SigningSecret)

	cpID := uuid.New().String()
	pubID := fmt.Sprintf("ack_%s", strings.ReplaceAll(cpID, "-", "")[:24])

	cp := contracts.CheckpointRecord{
		ID:              cpID,
		PublicID:        pubID,
		TenantID:        norm.TenantID,
		SourceService:   norm.SourceService,
		ExternalID:      uuid.New().String(),
		CorrelationID:   norm.CorrelationID,
		ShardID:         norm.ShardID,
		CheckpointSeq:   cpSeq,
		StartSequence:   startSeq,
		EndSequence:     latestRec.SequenceNumber,
		RecordCount:     int64(len(records)),
		MerkleRootHash:  merkleRoot,
		HeadRecordHash:  latestRec.RecordHash,
		AnchorSignature: sig,
		Status:          "ANCHORED",
		CreatedAt:       time.Now().UTC(),
	}

	if err := s.repo.AppendCheckpoint(ctx, cp); err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &cp, nil
}

func (s *AuditTrailService) VerifyShardIntegrity(ctx context.Context, in contracts.VerifyShardIntegrityInput) (*contracts.VerificationReport, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.VerifyShardIntegrity")
	defer span.End()

	norm, err := contracts.NormalizeVerifyShardIntegrityInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	latestCP, err := s.repo.GetLatestCheckpoint(ctx, norm.TenantID, norm.ShardID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if latestCP == nil {
		return &contracts.VerificationReport{
			TenantID: norm.TenantID,
			ShardID:  norm.ShardID,
			Status:   contracts.StatusUnknown,
			Message:  "No anchored checkpoint found for shard",
		}, nil
	}

	records, err := s.repo.GetShardRecordsRange(ctx, norm, 1, latestCP.EndSequence)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	currentPrevHash := GenesisPrevHash
	for _, rec := range records {
		expectedHash := rules.ComputeRecordHash(currentPrevHash, rec.CanonicalPayload, s.signingSecret)
		if rec.RecordHash != expectedHash {
			return &contracts.VerificationReport{
				TenantID:           norm.TenantID,
				ShardID:            norm.ShardID,
				Status:             contracts.StatusMismatch,
				LastSequenceNumber: rec.SequenceNumber,
				TotalRecords:       int64(len(records)),
				CalculatedHeadHash: expectedHash,
				AnchoredHeadHash:   latestCP.HeadRecordHash,
				TamperedSequence:   rec.SequenceNumber,
				Message:            fmt.Sprintf("Hash chain mismatch at sequence %d", rec.SequenceNumber),
			}, nil
		}
		currentPrevHash = rec.RecordHash
	}

	status := machines.ClassifyVerificationOutcome(currentPrevHash, latestCP.HeadRecordHash, int64(len(records)), latestCP.EndSequence)

	return &contracts.VerificationReport{
		TenantID:           norm.TenantID,
		ShardID:            norm.ShardID,
		Status:             status,
		LastSequenceNumber: int64(len(records)),
		TotalRecords:       int64(len(records)),
		CalculatedHeadHash: currentPrevHash,
		AnchoredHeadHash:   latestCP.HeadRecordHash,
		Message:            "Cryptographic verification succeeded",
	}, nil
}

func (s *AuditTrailService) QueryAudits(ctx context.Context, in contracts.QueryAuditInput) ([]contracts.AuditRecord, error) {
	ctx, span := s.tracer.Start(ctx, "AuditTrailService.QueryAudits")
	defer span.End()

	norm, err := contracts.NormalizeQueryAuditInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	accessID := uuid.New().String()
	pubID := fmt.Sprintf("ata_%s", strings.ReplaceAll(accessID, "-", "")[:24])
	logRec := contracts.AccessLogRecord{
		ID:            accessID,
		PublicID:      pubID,
		TenantID:      norm.TenantID,
		SourceService: "audit-query-service",
		ExternalID:    uuid.New().String(),
		CorrelationID: uuid.New().String(),
		AuditorID:     norm.AuditorID,
		CaseID:        norm.CaseID,
		QueryScope:    fmt.Sprintf("shard:%s,target:%s:%s", norm.ShardID, norm.TargetType, norm.TargetID),
		Reason:        norm.Reason,
		AccessedAt:    time.Now().UTC(),
	}

	if err := s.repo.RecordAccessLog(ctx, logRec); err != nil {
		span.RecordError(err)
		return nil, err
	}

	return s.repo.QueryAuditRecords(ctx, norm)
}
