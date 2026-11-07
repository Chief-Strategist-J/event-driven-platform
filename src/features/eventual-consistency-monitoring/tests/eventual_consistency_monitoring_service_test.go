package tests

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/ports"
	"event-driven-platform/src/features/eventual-consistency-monitoring/service"
)

type fakeECMRepo struct {
	incidents map[uuid.UUID]*contracts.DriftIncidentRecord
	checkRuns []contracts.CheckRunRecord
}

func (f *fakeECMRepo) UpsertWatermark(context.Context, contracts.WatermarkRecord) error { return nil }
func (f *fakeECMRepo) GetWatermarksByView(context.Context, string, string) ([]contracts.WatermarkRecord, error) {
	return nil, nil
}
func (f *fakeECMRepo) RecordDriftIncident(context.Context, contracts.DriftIncidentRecord) error {
	return nil
}
func (f *fakeECMRepo) GetActiveDriftIncidents(context.Context, string, string) ([]contracts.DriftIncidentRecord, error) {
	return nil, nil
}
func (f *fakeECMRepo) GetDriftIncident(_ context.Context, id uuid.UUID, _ string) (*contracts.DriftIncidentRecord, error) {
	if inc, ok := f.incidents[id]; ok {
		return inc, nil
	}
	return nil, ports.ErrIncidentNotFound
}
func (f *fakeECMRepo) UpdateDriftIncidentStatus(_ context.Context, id uuid.UUID, _ string, st contracts.IncidentStatus, attempts int) error {
	inc, ok := f.incidents[id]
	if !ok {
		return ports.ErrIncidentNotFound
	}
	inc.Status, inc.HealAttempts = st, attempts
	return nil
}
func (f *fakeECMRepo) RecordCheckRun(_ context.Context, r contracts.CheckRunRecord) error {
	f.checkRuns = append(f.checkRuns, r)
	return nil
}
func (f *fakeECMRepo) ListCheckRuns(context.Context, string, string, int) ([]contracts.CheckRunRecord, error) {
	return nil, nil
}
func (f *fakeECMRepo) RegisterSilence(context.Context, contracts.SilenceRecord) error { return nil }
func (f *fakeECMRepo) GetActiveSilence(context.Context, string, string) (*contracts.SilenceRecord, error) {
	return nil, nil
}

func TestAttemptHealing_NotFound(t *testing.T) {
	svc := service.NewConsistencyService(&fakeECMRepo{incidents: map[uuid.UUID]*contracts.DriftIncidentRecord{}})
	healed, err := svc.AttemptHealing(context.Background(), uuid.New(), "t1", 2)
	assert.ErrorIs(t, err, ports.ErrIncidentNotFound)
	assert.False(t, healed)
}

func TestAttemptHealing_UsesPersistedAttempts(t *testing.T) {
	id := uuid.New()
	repo := &fakeECMRepo{incidents: map[uuid.UUID]*contracts.DriftIncidentRecord{
		id: {ID: id, Status: contracts.IncidentConfirmed, HealAttempts: 2},
	}}
	svc := service.NewConsistencyService(repo)

	healed, err := svc.AttemptHealing(context.Background(), id, "t1", 2)
	require.NoError(t, err)
	assert.False(t, healed)
	assert.Equal(t, contracts.IncidentManualIntervention, repo.incidents[id].Status)

	id2 := uuid.New()
	repo.incidents[id2] = &contracts.DriftIncidentRecord{ID: id2, Status: contracts.IncidentConfirmed, HealAttempts: 0}
	healed, err = svc.AttemptHealing(context.Background(), id2, "t1", 2)
	require.NoError(t, err)
	assert.True(t, healed)
	assert.Equal(t, 1, repo.incidents[id2].HealAttempts)
}

func TestDispatchCanaryProbe_RecordedAndNotClaimedMatch(t *testing.T) {
	repo := &fakeECMRepo{}
	svc := service.NewConsistencyService(repo)
	out, err := svc.DispatchCanaryProbe(context.Background(), contracts.CanaryProbeInput{
		TenantID: "t1", SourceService: "svc", StreamName: "orders", PartitionID: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.OutcomeUnknown, out.Outcome)
	require.Len(t, repo.checkRuns, 1)
	assert.Equal(t, contracts.CheckCanaryProbe, repo.checkRuns[0].CheckType)
}
