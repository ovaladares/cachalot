package cachalot_test

import (
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

type MockElectionManager struct {
	StartElectionCalledWith []*domain.Event
	HandleVoteCalledWith    []*domain.Event
	HandleVoteErr           error
}

func (m *MockElectionManager) StartElection(event *domain.Event) {
	m.StartElectionCalledWith = append(m.StartElectionCalledWith, event)
}

func (m *MockElectionManager) HandleVote(event *domain.Event) error {
	m.HandleVoteCalledWith = append(m.HandleVoteCalledWith, event)

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

	PendingLockCalledWith     []string
	PendingLockCallCount      int
	PendingLockResponse       chan string
	PendingLockResponseExists bool
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
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.PendingLockCalledWith = append(m.PendingLockCalledWith, key)
	m.PendingLockCallCount++

	return m.PendingLockResponse, m.PendingLockResponseExists
}

func (m *MockLockManager) DeletePendingLock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeletePendingLockCalledWith = append(m.DeletePendingLockCalledWith, key)
	m.DeletePendingLockCallCount++

	if m.locks == nil {
		return
	}
}

type BroadcastEventInput struct {
	EventName string
	Data      []byte
}

type MockClusterManager struct {
	NodeID                   string
	BroadcastEventCalledWith []BroadcastEventInput
	BroadcastEventErr        error
	GetMembersCallCount      int
	GetMembersResponse       []*discovery.Member
	GetMembersErr            error

	GetMembersCountCallCount int
	GetMembersCountResponse  int
	GetMembersCountErr       error
}

func (m *MockClusterManager) GetMembers() ([]*discovery.Member, error) {
	m.GetMembersCallCount++
	if m.GetMembersErr != nil {
		return nil, m.GetMembersErr
	}

	return m.GetMembersResponse, nil
}

func (m *MockClusterManager) GetMembersCount() (int, error) {
	m.GetMembersCountCallCount++
	if m.GetMembersCountErr != nil {
		return 0, m.GetMembersCountErr
	}

	return m.GetMembersCountResponse, nil
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
