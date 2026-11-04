package contracts

import (
	"encoding/json"
	"time"
)

type MemberStatus string

const (
	MemberStatusJoining     MemberStatus = "JOINING"
	MemberStatusStable      MemberStatus = "STABLE"
	MemberStatusRebalancing MemberStatus = "REBALANCING"
	MemberStatusDead        MemberStatus = "DEAD"
)

type PartitionAssignment struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Epoch     int64  `json:"epoch"`
}

type ConsumerGroupMemberRecord struct {
	ID                 string          `json:"id"`
	PublicID           string          `json:"publicId"`
	TenantID           string          `json:"tenantId"`
	SourceService      string          `json:"sourceService"`
	ExternalID         string          `json:"externalId"`
	CorrelationID      string          `json:"correlationId"`
	GroupID            string          `json:"groupId"`
	MemberID           string          `json:"memberId"`
	InstanceID         *string         `json:"instanceId,omitempty"`
	ClientHost         string          `json:"clientHost"`
	AssignmentStrategy string          `json:"assignmentStrategy"`
	AssignedPartitions json.RawMessage `json:"assignedPartitions"`
	GenerationID       int64           `json:"generationId"`
	HeartbeatAt        time.Time       `json:"heartbeatAt"`
	Status             MemberStatus    `json:"status"`
	CreatedAt          time.Time       `json:"createdAt"`
	UpdatedAt          time.Time       `json:"updatedAt"`
}

type RebalanceLogRecord struct {
	ID                 string          `json:"id"`
	PublicID           string          `json:"publicId"`
	TenantID           string          `json:"tenantId"`
	SourceService      string          `json:"sourceService"`
	ExternalID         string          `json:"externalId"`
	CorrelationID      string          `json:"correlationId"`
	GroupID            string          `json:"groupId"`
	GenerationID       int64           `json:"generationId"`
	RebalanceReason    string          `json:"rebalanceReason"`
	MemberCount        int             `json:"memberCount"`
	RevokedPartitions  json.RawMessage `json:"revokedPartitions"`
	AssignedPartitions json.RawMessage `json:"assignedPartitions"`
	DurationMs         int64           `json:"durationMs"`
	CreatedAt          time.Time       `json:"createdAt"`
}

type RegisterMemberInput struct {
	TenantID           string  `json:"tenantId"`
	SourceService      string  `json:"sourceService"`
	ExternalID         string  `json:"externalId"`
	CorrelationID      string  `json:"correlationId,omitempty"`
	GroupID            string  `json:"groupId"`
	MemberID           string  `json:"memberId"`
	InstanceID         *string `json:"instanceId,omitempty"`
	ClientHost         string  `json:"clientHost"`
	AssignmentStrategy string  `json:"assignmentStrategy"`
}

type HeartbeatInput struct {
	TenantID      string  `json:"tenantId"`
	GroupID       string  `json:"groupId"`
	MemberID      string  `json:"memberId"`
	InstanceID    *string `json:"instanceId,omitempty"`
	CorrelationID string  `json:"correlationId,omitempty"`
}

type HeartbeatResult struct {
	Status       MemberStatus `json:"status"`
	GenerationID int64        `json:"generationId"`
	MustRejoin   bool         `json:"mustRejoin"`
}

type RebalanceSyncInput struct {
	TenantID           string                `json:"tenantId"`
	SourceService      string                `json:"sourceService"`
	ExternalID         string                `json:"externalId"`
	CorrelationID      string                `json:"correlationId,omitempty"`
	GroupID            string                `json:"groupId"`
	MemberID           string                `json:"memberId"`
	GenerationID       int64                 `json:"generationId"`
	RebalanceReason    string                `json:"rebalanceReason"`
	MemberCount        int                   `json:"memberCount"`
	RevokedPartitions  []PartitionAssignment `json:"revokedPartitions"`
	AssignedPartitions []PartitionAssignment `json:"assignedPartitions"`
	DurationMs         int64                 `json:"durationMs"`
}
