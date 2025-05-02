package internal_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/otaviovaladares/cachalot/internal"
	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/stretchr/testify/assert"
)

type MockElectionManager struct {
	ClaimKeyCalledWith      []*internal.Event
	HandleKeyVoteCalledWith []*internal.Event
	HandleKeyVoteErr        error
}

func (m *MockElectionManager) ClaimKey(event *internal.Event) {
	m.ClaimKeyCalledWith = append(m.ClaimKeyCalledWith, event)
}

func (m *MockElectionManager) HandleKeyVote(event *internal.Event) error {
	m.HandleKeyVoteCalledWith = append(m.HandleKeyVoteCalledWith, event)

	return nil
}

func (m *MockElectionManager) VoteForKey(_ *internal.Event) error {
	panic("implement me")
}

func (m *MockElectionManager) AcquireLock(_ *internal.Lock) error {
	panic("implement me")
}

func (m *MockElectionManager) DeleteProposal(_ string) {
	panic("implement me")
}

type MockLockManager struct {
	locks map[string]string
	mutex sync.RWMutex
}

func (m *MockLockManager) AcquireLock(key, nodeID string, duration time.Duration) (chan string, error) {
	panic("implement me")
}

func (m *MockLockManager) GetLocks() (map[string]string, error) {
	panic("implement me")
}

func (m *MockLockManager) IsLocked(key string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.locks == nil {
		return false
	}

	_, exists := m.locks[key]
	return exists
}

func (m *MockLockManager) SetLock(key, nodeID string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	m.locks[key] = nodeID
	return true
}

func (m *MockLockManager) PendingLock(key string) (chan string, bool) {
	panic("implement me")
}

func (m *MockLockManager) DeletePendingLock(key string) {
	panic("implement me")
}

func TestEventHandlerHandle_HandleAcquireLockEvent(t *testing.T) {
	lockEvent := &internal.Lock{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(lockEvent)
	if err != nil {
		t.Fatalf("failed to marshal lock event: %v", err)
	}

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := internal.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: internal.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
	assert.Equal(t, 1, len(lockManager.locks), "expected one lock to be acquired")
}

func TestEventHandlerHandle_HandleAcquireLockEventAlreadyLocked(t *testing.T) {
	lockEvent := &internal.Lock{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(lockEvent)
	if err != nil {
		t.Fatalf("failed to marshal lock event: %v", err)
	}

	lockManager := &MockLockManager{
		locks: map[string]string{
			lockEvent.Key: lockEvent.NodeID,
		},
	}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := internal.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: internal.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
}

func TestEventHandlerHandle_VoteForKeySuccess(t *testing.T) {
	voteEvent := &internal.Event{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(voteEvent)
	if err != nil {
		t.Fatalf("failed to marshal lock event: %v", err)
	}

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := internal.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: internal.VoteForKeyEventName,
		Body: b,
	})

	assert.Equal(t, 1, len(electionManager.HandleKeyVoteCalledWith), "expected one vote event to be handled")
	assert.Equal(t, voteEvent, electionManager.HandleKeyVoteCalledWith[0], "expected vote event to be handled")
}

func TestEventHandlerHandle_ClaimKeySuccess(t *testing.T) {
	claimEvent := &internal.Event{
		Key:    "test-key",
		NodeID: "test-node",
	}

	b, err := json.Marshal(claimEvent)
	if err != nil {
		t.Fatalf("failed to marshal lock event: %v", err)
	}

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := internal.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: internal.ClaimKeyEventName,
		Body: b,
	})

	assert.Equal(t, 1, len(electionManager.ClaimKeyCalledWith), "expected one claim event to be handled")
	assert.Equal(t, claimEvent, electionManager.ClaimKeyCalledWith[0], "expected claim event to be handled")
}
