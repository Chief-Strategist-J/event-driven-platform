package contracts

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func GeneratePublicID(prefix string) (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate secure random id: %w", err)
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b)), nil
}

func ValidateHopPublicID(publicID string) bool {
	return HopPublicIDRegex.MatchString(publicID)
}

func ValidateItlPublicID(publicID string) bool {
	return ItlPublicIDRegex.MatchString(publicID)
}

func NormalizeReportHopTelemetryInput(in *ReportHopTelemetryInput) (*PipelineHopRecord, error) {
	if in == nil {
		return nil, fmt.Errorf("input must not be nil")
	}
	tID := strings.TrimSpace(in.TenantID)
	if tID == "" {
		return nil, ErrInvalidTenantID
	}
	src := strings.TrimSpace(in.SourceService)
	if src == "" {
		return nil, ErrInvalidSourceService
	}
	ext := strings.TrimSpace(in.ExternalID)
	if ext == "" {
		return nil, ErrInvalidExternalID
	}
	flow := strings.TrimSpace(in.FlowName)
	if flow == "" {
		return nil, ErrInvalidFlowName
	}
	switch in.HopCode {
	case HopH1Outbox, HopH2Relay, HopH3Kafka, HopH4Consumer, HopH5AuthView, HopH6DerivedStore:
	default:
		return nil, ErrInvalidHopCode
	}
	switch in.Tier {
	case Tier0, Tier1, Tier2:
	default:
		return nil, ErrInvalidTier
	}
	switch in.Status {
	case StatusOk, StatusLagging, StatusStalled, StatusStopped, StatusPaused, StatusUnknown:
	default:
		return nil, ErrInvalidStatus
	}
	pReason := in.PauseReason
	if pReason == "" {
		pReason = PauseReasonNone
	}
	switch pReason {
	case PauseReasonNone, PauseReasonBreakerOpen, PauseReasonBackpressure, PauseReasonOperatorHalt, PauseReasonRebalance, PauseReasonCorruptHead:
	default:
		return nil, ErrInvalidPauseReason
	}
	if in.LagSeconds < 0 {
		return nil, ErrNegativeLag
	}
	if in.OldestHeldAgeSeconds < 0 {
		return nil, ErrNegativeHeldAge
	}
	runbook := strings.TrimSpace(in.RunbookURL)
	if runbook == "" {
		return nil, ErrInvalidRunbookURL
	}

	pubID, err := GeneratePublicID("hop")
	if err != nil {
		return nil, err
	}

	corrID := in.CorrelationID
	if corrID == uuid.Nil {
		corrID = uuid.New()
	}

	now := time.Now().UTC()
	return &PipelineHopRecord{
		ID:                   uuid.New(),
		PublicID:             pubID,
		TenantID:             tID,
		SourceService:        src,
		ExternalID:           ext,
		CorrelationID:        corrID,
		FlowName:             flow,
		HopCode:              in.HopCode,
		Tier:                 in.Tier,
		Status:               in.Status,
		PauseReason:          pReason,
		LagSeconds:           in.LagSeconds,
		OldestHeldAgeSeconds: in.OldestHeldAgeSeconds,
		RunbookURL:           runbook,
		LastReportedAt:       now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}, nil
}

func NormalizeTriageFlowInput(in *TriageFlowInput) (*TriageFlowInput, error) {
	if in == nil {
		return nil, fmt.Errorf("input must not be nil")
	}
	tID := strings.TrimSpace(in.TenantID)
	if tID == "" {
		return nil, ErrInvalidTenantID
	}
	src := strings.TrimSpace(in.SourceService)
	if src == "" {
		return nil, ErrInvalidSourceService
	}
	ext := strings.TrimSpace(in.ExternalID)
	if ext == "" {
		return nil, ErrInvalidExternalID
	}
	inc := strings.TrimSpace(in.IncidentID)
	if inc == "" {
		return nil, ErrInvalidIncidentID
	}
	flow := strings.TrimSpace(in.FlowName)
	if flow == "" {
		return nil, ErrInvalidFlowName
	}
	by := strings.TrimSpace(in.TriagedBy)
	if by == "" {
		by = "system_oncall"
	}

	corrID := in.CorrelationID
	if corrID == uuid.Nil {
		corrID = uuid.New()
	}

	return &TriageFlowInput{
		TenantID:      tID,
		SourceService: src,
		ExternalID:    ext,
		CorrelationID: corrID,
		IncidentID:    inc,
		FlowName:      flow,
		TriagedBy:     by,
	}, nil
}
