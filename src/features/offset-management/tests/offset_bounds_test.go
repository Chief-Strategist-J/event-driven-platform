package tests

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/infrastructure/adapters"
	"event-driven-platform/src/features/offset-management/service"
	appErrors "event-driven-platform/src/shared/errors"
)

func TestRewindAndCommitBounds(t *testing.T) {
	ctx := context.Background()
	svc := service.NewOffsetManagementService(adapters.NewMemoryOffsetManagementAdapter())
	const corr = "018f3a20-8b9a-7c22-b25b-f350c316cf5a"

	_, err := svc.CommitOffset(ctx, contracts.CommitOffsetInput{
		TenantID: "t", SourceService: "svc", ExternalID: "c1", CorrelationID: corr,
		GroupID: "g", Topic: "tp", Partition: 0, Offset: 100, LogEndOffset: 120,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	rewind := func(group, topic string, part int32, target int64) error {
		_, err := svc.RewindOffset(ctx, contracts.RewindOffsetInput{
			TenantID: "t", SourceService: "svc", ExternalID: "r", CorrelationID: corr,
			GroupID: group, Topic: topic, Partition: part, TargetOffset: target, Reason: "test",
		})
		return err
	}
	status := func(err error) int {
		var ae *appErrors.ApplicationError
		if errors.As(err, &ae) {
			return ae.StatusCode
		}
		return 0
	}

	if got := status(rewind("g", "tp", 0, 110)); got != http.StatusBadRequest {
		t.Fatalf("forward jump: want 400, got %d", got)
	}
	if got := status(rewind("g", "tp", 0, 500)); got != http.StatusBadRequest {
		t.Fatalf("beyond log end: want 400, got %d", got)
	}
	if got := status(rewind("nope", "tp", 0, 1)); got != http.StatusNotFound {
		t.Fatalf("unknown group: want 404, got %d", got)
	}
	if got := status(rewind("g", "tp", 7, 1)); got != http.StatusNotFound {
		t.Fatalf("unknown partition: want 404, got %d", got)
	}
	if err := rewind("g", "tp", 0, 100); err != nil {
		t.Fatalf("rewind to current offset should be allowed: %v", err)
	}
	if err := rewind("g", "tp", 0, 10); err != nil {
		t.Fatalf("rewind backwards: %v", err)
	}

	bad := []contracts.CommitOffsetInput{
		{TenantID: "t", SourceService: "svc", ExternalID: "x", CorrelationID: corr, GroupID: "", Topic: "tp", Offset: 1, LogEndOffset: 2},
		{TenantID: "t", SourceService: "svc", ExternalID: "x", CorrelationID: corr, GroupID: "g", Topic: "tp", Partition: -1, Offset: 1, LogEndOffset: 2},
		{TenantID: "t", SourceService: "svc", ExternalID: "", CorrelationID: corr, GroupID: "g", Topic: "tp", Offset: 1, LogEndOffset: 2},
		{TenantID: "t", SourceService: "svc", ExternalID: "x", CorrelationID: "not-a-uuid", GroupID: "g", Topic: "tp", Offset: 1, LogEndOffset: 2},
		{TenantID: "t", SourceService: "svc", ExternalID: "x", CorrelationID: corr, GroupID: "g", Topic: "tp", Offset: 5, LogEndOffset: 2},
	}
	for i, in := range bad {
		if _, err := svc.CommitOffset(ctx, in); status(err) != http.StatusBadRequest {
			t.Fatalf("bad commit %d: want 400, got %v", i, err)
		}
	}
}
