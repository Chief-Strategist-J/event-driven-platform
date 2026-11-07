package tests

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/infrastructure/ports"
	"event-driven-platform/src/features/materialized-views/service"
	appErrors "event-driven-platform/src/shared/errors"
)

type missingViewRepo struct {
	ports.ViewRepositoryPort
}

func (missingViewRepo) GetViewByName(_ context.Context, _ string, _ string) (*contracts.ViewRegistryRecord, error) {
	return nil, appErrors.NewNotFoundError("materialized view registry record not found")
}

func TestQueryViewUnknownViewIsNotFound(t *testing.T) {
	svc := service.NewViewService(missingViewRepo{})

	_, err := svc.QueryViewWithConsistencyToken(context.Background(), contracts.QueryViewInput{
		TenantID: "t", ViewName: "nope", AggregateType: "order", AggregateID: "1",
	})
	var appErr *appErrors.ApplicationError
	if !errors.As(err, &appErr) || appErr.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %v", err)
	}

	_, err = svc.QueryViewWithConsistencyToken(context.Background(), contracts.QueryViewInput{ViewName: "x"})
	if !errors.As(err, &appErr) || appErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing tenant, got %v", err)
	}
}
