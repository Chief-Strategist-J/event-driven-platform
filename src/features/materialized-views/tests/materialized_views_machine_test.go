package tests

import (
	"testing"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/domain/machines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewRebuildStateMachine_ValidTransitions(t *testing.T) {
	sm := machines.NewViewRebuildStateMachine()

	next, err := sm.Transition(contracts.RebuildPhaseInitiated, contracts.RebuildPhaseHistoricalReplay)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseHistoricalReplay, next)

	next, err = sm.Transition(next, contracts.RebuildPhaseStreamCatchup)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseStreamCatchup, next)

	next, err = sm.Transition(next, contracts.RebuildPhasePromoted)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhasePromoted, next)

	next, err = sm.Transition(next, contracts.RebuildPhaseDecommissioned)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseDecommissioned, next)
}

func TestViewRebuildStateMachine_AbortsToDecommissioned(t *testing.T) {
	sm := machines.NewViewRebuildStateMachine()

	next, err := sm.Transition(contracts.RebuildPhaseInitiated, contracts.RebuildPhaseDecommissioned)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseDecommissioned, next)

	next, err = sm.Transition(contracts.RebuildPhaseHistoricalReplay, contracts.RebuildPhaseDecommissioned)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseDecommissioned, next)

	next, err = sm.Transition(contracts.RebuildPhaseStreamCatchup, contracts.RebuildPhaseDecommissioned)
	require.NoError(t, err)
	assert.Equal(t, contracts.RebuildPhaseDecommissioned, next)
}

func TestViewRebuildStateMachine_InvalidTransitions(t *testing.T) {
	sm := machines.NewViewRebuildStateMachine()

	_, err := sm.Transition(contracts.RebuildPhaseInitiated, contracts.RebuildPhasePromoted)
	assert.ErrorIs(t, err, machines.ErrInvalidRebuildTransition)

	_, err = sm.Transition(contracts.RebuildPhaseHistoricalReplay, contracts.RebuildPhaseInitiated)
	assert.ErrorIs(t, err, machines.ErrInvalidRebuildTransition)

	_, err = sm.Transition(contracts.RebuildPhaseDecommissioned, contracts.RebuildPhaseInitiated)
	assert.ErrorIs(t, err, machines.ErrRebuildTerminalState)
}
