package cachalot_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	cachalot "github.com/otaviovaladares/cachalot/pkg"
	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestEventHandlerHandle_HandleAcquireLockEvent(t *testing.T) {
	lockEvent := &storage.Lock{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(lockEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

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

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
}

func TestEventHandlerHandle_VoteForKeySuccess(t *testing.T) {
	voteEvent := &domain.Event{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(voteEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.VoteForKeyEventName,
		Body: b,
	})

	assert.Len(t, electionManager.HandleVoteCalledWith, 1, "expected one vote event to be handled")
	assert.Equal(t, voteEvent, electionManager.HandleVoteCalledWith[0], "expected vote event to be handled")
}

func TestEventHandlerHandle_ClaimKeySuccess(t *testing.T) {
	claimEvent := &domain.Event{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(claimEvent)
	assert.NoError(t, err, "failed to marshal claim event")

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

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

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.RenewLockEventName,
		Body: b,
	})

	assert.Len(t, lockManager.RenewLockCalledWith, 1, "expected renew to be called once")
	assert.Equal(t, renewEvent.Key, lockManager.RenewLockCalledWith[0], "expected renew to be called with the correct key")
}
