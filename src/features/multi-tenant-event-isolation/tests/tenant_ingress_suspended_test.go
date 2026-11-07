package tests

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/rules"
	"event-driven-platform/src/features/multi-tenant-event-isolation/service"
	appErrors "event-driven-platform/src/shared/errors"
)

type fakeTenantRepo struct {
	tenant *contracts.TenantRecord
}

func (f *fakeTenantRepo) RegisterTenant(context.Context, contracts.TenantRecord) error { return nil }
func (f *fakeTenantRepo) GetTenant(context.Context, string) (*contracts.TenantRecord, error) {
	return f.tenant, nil
}
func (f *fakeTenantRepo) UpdateLifecycleState(context.Context, string, contracts.TenantLifecycleState, int64) error {
	return nil
}
func (f *fakeTenantRepo) UpdateQuotaBudget(context.Context, string, int, int, bool) error { return nil }
func (f *fakeTenantRepo) GetQuotaBudget(context.Context, string) (*contracts.QuotaCheckResult, error) {
	return &contracts.QuotaCheckResult{Allowed: true}, nil
}
func (f *fakeTenantRepo) AppendAuditLog(context.Context, string, string, string, string, string, map[string]interface{}) error {
	return nil
}

func TestVerifyIngressEnvelope_LifecycleGate(t *testing.T) {
	env := contracts.VerifyEnvelopeInput{HeaderTenant: "t1", KeyPrefix: "t1", PayloadTenant: "t1", CurrentRegion: "eu-west-1"}
	cases := []struct {
		state      contracts.TenantLifecycleState
		wantStatus int // 0 = accepted
	}{
		{contracts.LifecycleActive, 0},
		{contracts.LifecycleSuspended, http.StatusForbidden},
		{contracts.LifecycleOffboarded, http.StatusConflict},
	}
	for _, c := range cases {
		repo := &fakeTenantRepo{tenant: &contracts.TenantRecord{TenantID: "t1", LifecycleState: c.state, AllowedRegions: []string{"eu-west-1"}}}
		svc := service.NewMultiTenantService(repo, nil, machines.NewTenantLifecycleMachine())
		err := svc.VerifyIngressEnvelope(context.Background(), env)
		if c.wantStatus == 0 {
			if err != nil {
				t.Fatalf("%s: expected accept, got %v", c.state, err)
			}
			continue
		}
		var appErr *appErrors.ApplicationError
		if !errors.As(err, &appErr) || appErr.StatusCode != c.wantStatus {
			t.Fatalf("%s: expected status %d, got %v", c.state, c.wantStatus, err)
		}
	}
	if !errors.Is(rules.ErrTenantSuspended, rules.ErrTenantSuspended) {
	}
}
