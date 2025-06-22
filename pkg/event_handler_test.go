package cachalot_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	cachalot "github.com/ovaladares/cachalot/pkg"
	"github.com/ovaladares/cachalot/pkg/discovery"
	"github.com/ovaladares/cachalot/pkg/domain"
	"github.com/ovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestEventHandlerHandle_HandleAcquireLockEvent(t *testing.T) {
	lockEvent := &domain.AcquireLockEvent{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(lockEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
	assert.Len(t, lockManager.locks, 1, "expected one lock to be acquired")

	assert.Equal(t, 1, lockManager.DeletePendingLockCallCount, "expected DeletePendingLock to be called once")
	assert.Equal(t, lockEvent.Key, lockManager.DeletePendingLockCalledWith[0], "expected DeletePendingLock to be called with the correct key")
}

func TestEventHandlerHandle_HandleAcquireLockEventAlreadyLocked(t *testing.T) {
	lockEvent := &storage.Lock{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(lockEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{
		locks: map[string]string{
			lockEvent.Key: lockEvent.NodeID,
		},
	}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
}

func TestEventHandlerHandle_VoteForKeySuccess(t *testing.T) {
	voteEvent := &domain.VoteForKeyEvent{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(voteEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.VoteForKeyEventName,
		Body: b,
	})

	assert.Len(t, electionManager.HandleVoteCalledWith, 1, "expected one vote event to be handled")
	assert.Equal(t, voteEvent, electionManager.HandleVoteCalledWith[0], "expected vote event to be handled")
}

func TestEventHandlerHandle_ClaimKeySuccess(t *testing.T) {
	claimEvent := &domain.ClaimKeyEvent{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(claimEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.ClaimKeyEventName,
		Body: b,
	})

	assert.Len(t, electionManager.StartElectionCalledWith, 1, "expected one claim event to be handled")
	assert.Equal(t, claimEvent, electionManager.StartElectionCalledWith[0], "expected claim event to be handled")
}

func TestEventHandlerHandle_RenewLockEvent(t *testing.T) {
	renewEvent := &domain.Event{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(renewEvent)
	assert.NoError(t, err, "failed to marshal renew event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.RenewLockEventName,
		Body: b,
	})

	assert.Len(t, lockManager.RenewLockCalledWith, 1, "expected renew to be called once")
	assert.Equal(t, renewEvent.Key, lockManager.RenewLockCalledWith[0], "expected renew to be called with the correct key")
}

func TestEventHandlerHandle_ReleaseLockEvent(t *testing.T) {
	releaseEvent := &domain.ReleaseLockEvent{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(releaseEvent)
	assert.NoError(t, err, "failed to marshal release event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.ReleaseLockEventName,
		Body: b,
	})

	assert.Len(t, lockManager.ReleaseLockCalledWith, 1, "expected release to be called once")
	assert.Equal(t, releaseEvent.Key, lockManager.ReleaseLockCalledWith[0], "expected release to be called with the correct key")
}

func TestEventHandlerHandle_LocksRequestEvent(t *testing.T) {
	locksRequestEvent := &domain.LocksRequestEvent{
		NodeID: "test-node2",
	}

	b, err := json.Marshal(locksRequestEvent)
	assert.NoError(t, err, "failed to marshal locks request event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LocksRequestEventName,
		Body: b,
	})

	assert.Equal(t, 1, lockManager.DumpLocksCallCount, "expected DumpLocks to be called once")
}

func TestEventHandlerHandle_LocksRequestEventSameNodeID(t *testing.T) {
	locksRequestEvent := &domain.LocksRequestEvent{
		NodeID: "test-node",
	}

	b, err := json.Marshal(locksRequestEvent)
	assert.NoError(t, err, "failed to marshal locks request event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LocksRequestEventName,
		Body: b,
	})

	assert.Equal(t, 0, lockManager.DumpLocksCallCount, "expected DumpLocks to not be called")
}

func TestHandlerHandle_LocksSnapshotEvent(t *testing.T) {
	locksSnapshotEvent := &domain.LocksSnapShotEvent{
		NodeID: "test-node2",
		Locks: map[string]domain.LockSnapshot{
			"test-key": {
				Key:        "test-key",
				NodeID:     "test-node2",
				TimeMillis: 1234567890,
			},
		},
	}

	b, err := json.Marshal(locksSnapshotEvent)
	assert.NoError(t, err, "failed to marshal locks snapshot event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LocksSnapShotEventName,
		Body: b,
	})

	assert.Len(t, snapshotManager.AddSnapshotCalledWith, 1, "expected one locks snapshot event to be handled")
	assert.Equal(t, locksSnapshotEvent, snapshotManager.AddSnapshotCalledWith[0], "expected locks snapshot event to be handled")
}

func TestHandlerHandle_LocksSnapshotEventSameNodeID(t *testing.T) {
	locksSnapshotEvent := &domain.LocksSnapShotEvent{
		NodeID: "test-node",
		Locks: map[string]domain.LockSnapshot{
			"test-key": {
				Key:        "test-key",
				NodeID:     "test-node2",
				TimeMillis: 1234567890,
			},
		},
	}

	b, err := json.Marshal(locksSnapshotEvent)
	assert.NoError(t, err, "failed to marshal locks snapshot event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}
	snapshotManager := &MockSnapshotManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, snapshotManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LocksSnapShotEventName,
		Body: b,
	})

	assert.Len(t, snapshotManager.AddSnapshotCalledWith, 0, "expected to not add snapshot locks")
}
