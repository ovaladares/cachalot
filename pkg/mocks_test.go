package cachalot_test

import (
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
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

	RenewLockCalledWith []string
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	m.RenewLockCalledWith = append(m.RenewLockCalledWith, key)

	return nil
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

type BroadcastEventInput struct {
	EventName string
	Data      []byte
}

type MockClusterManager struct {
	NodeID                   string
	BroadcastEventCalledWith []BroadcastEventInput
	BroadcastEventErr        error
}

func (m *MockClusterManager) GetMembers() ([]*discovery.Member, error) {
	return nil, nil
}

func (m *MockClusterManager) GetMembersCount() (int, error) {
	return 0, nil
}

func (m *MockClusterManager) Connect() error {
	return nil
}
func (m *MockClusterManager) Disconnect() error {
	return nil
}
func (m *MockClusterManager) GetNodeID() string {
	return m.NodeID
}

func (m *MockClusterManager) BroadcastEvent(event string, data []byte) error {
	m.BroadcastEventCalledWith = append(m.BroadcastEventCalledWith, BroadcastEventInput{
		EventName: event,
		Data:      data,
	})

	return m.BroadcastEventErr
}

func (m *MockClusterManager) RegisterEventHandler(handler func(*discovery.ClusterEvent)) error {
	return nil
}
