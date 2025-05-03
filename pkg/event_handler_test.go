package cachalot_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	cachalot "github.com/otaviovaladares/cachalot/pkg"
	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

type MockElectionManager struct {
	ClaimKeyCalledWith      []*domain.Event
	HandleKeyVoteCalledWith []*domain.Event
	HandleKeyVoteErr        error
}

func (m *MockElectionManager) ClaimKey(event *domain.Event) {
	m.ClaimKeyCalledWith = append(m.ClaimKeyCalledWith, event)
}

func (m *MockElectionManager) HandleKeyVote(event *domain.Event) error {
	m.HandleKeyVoteCalledWith = append(m.HandleKeyVoteCalledWith, event)

	return nil
}

func (m *MockElectionManager) VoteForKey(_ *domain.Event) error {
	panic("implement me")
}

func (m *MockElectionManager) AcquireLock(_ *storage.Lock) error {
	panic("implement me")
}

func (m *MockElectionManager) DeleteProposal(_ string) {
	panic("implement me")
}

type MockLockManager struct {
	locks map[string]string
	mu    sync.RWMutex

	DeletePendingLockCalledWith []string
	DeletePendingLockCallCount  int
}

func (m *MockLockManager) AcquireLock(key, nodeID string, duration time.Duration) (chan string, error) {
	panic("implement me")
}

func (m *MockLockManager) GetLocks() (map[string]string, error) {
	panic("implement me")
}

func (m *MockLockManager) IsLocked(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.locks == nil {
		return false
	}

	_, exists := m.locks[key]
	return exists
}

func (m *MockLockManager) SetLock(key, nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	m.locks[key] = nodeID
	return true
}

func (m *MockLockManager) RenewLock(key string, durationMs int64) error {
	panic("implement me")
}

func (m *MockLockManager) Renew(key string, duration time.Duration) error {
	panic("implement me")
}

func (m *MockLockManager) PendingLock(key string) (chan string, bool) {
	panic("implement me")
}

func (m *MockLockManager) DeletePendingLock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.locks == nil {
		return
	}

	m.DeletePendingLockCalledWith = append(m.DeletePendingLockCalledWith, key)
	m.DeletePendingLockCallCount++
}

func TestEventHandlerHandle_HandleAcquireLockEvent(t *testing.T) {
	lockEvent := &storage.Lock{
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

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.LockAcquiredEventName,
		Body: b,
	})

	assert.True(t, lockManager.IsLocked(lockEvent.Key), "expected lock to be acquired")
	assert.Equal(t, lockEvent.NodeID, lockManager.locks[lockEvent.Key], "expected lock to be acquired by the correct node")
	assert.Equal(t, 1, len(lockManager.locks), "expected one lock to be acquired")
	assert.Equal(t, 1, lockManager.DeletePendingLockCallCount, "expected DeletePendingLock to be called once")
	assert.Equal(t, lockEvent.Key, lockManager.DeletePendingLockCalledWith[0], "expected DeletePendingLock to be called with the correct key")
}

func TestEventHandlerHandle_HandleAcquireLockEventAlreadyLocked(t *testing.T) {
	lockEvent := &storage.Lock{
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
	if err != nil {
		t.Fatalf("failed to marshal lock event: %v", err)
	}

	lockManager := &MockLockManager{}
	electionManager := &MockElectionManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.VoteForKeyEventName,
		Body: b,
	})

	assert.Equal(t, 1, len(electionManager.HandleKeyVoteCalledWith), "expected one vote event to be handled")
	assert.Equal(t, voteEvent, electionManager.HandleKeyVoteCalledWith[0], "expected vote event to be handled")
}

func TestEventHandlerHandle_ClaimKeySuccess(t *testing.T) {
	claimEvent := &domain.Event{
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

	eventHandler := cachalot.NewServiceDiscoveryEventHandler(lockManager, electionManager, "test-node", logg)

	eventHandler.Handle(&discovery.ClusterEvent{
		Type: domain.ClaimKeyEventName,
		Body: b,
	})

	assert.Equal(t, 1, len(electionManager.ClaimKeyCalledWith), "expected one claim event to be handled")
	assert.Equal(t, claimEvent, electionManager.ClaimKeyCalledWith[0], "expected claim event to be handled")
}
