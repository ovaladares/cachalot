package cachalot_test

import (
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

type MockSnapshotManager struct{}

func (m *MockSnapshotManager) SyncLocks() error {
	return nil
}

func (m *MockSnapshotManager) AddSnapshot(event *domain.LocksSnapShotEvent) error {
	return nil
}

// This file contains mock implementations of various interfaces used in the
// Cachalot package. These mocks are useful for unit testing and simulating
// different behaviors of the components without needing to rely on actual

type MockElectionManager struct {
	StartElectionCalledWith []*domain.ClaimKeyEvent
	HandleVoteCalledWith    []*domain.VoteForKeyEvent
	HandleVoteErr           error

	Mu sync.RWMutex
}

func (m *MockElectionManager) StartElection(event *domain.ClaimKeyEvent) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.StartElectionCalledWith = append(m.StartElectionCalledWith, event)
}

func (m *MockElectionManager) HandleVote(event *domain.VoteForKeyEvent) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.HandleVoteCalledWith = append(m.HandleVoteCalledWith, event)

	return nil
}

func (m *MockElectionManager) VoteForKey(_, _ string, _ int64, _ int) error {
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
	Mu    sync.RWMutex

	DeletePendingLockCalledWith []string
	DeletePendingLockCallCount  int

	RenewLockCalledWith []string

	PendingLockCalledWith     []string
	PendingLockCallCount      int
	PendingLockResponse       chan string
	PendingLockResponseExists bool

	ReleaseLockCallCount  int
	ReleaseLockCalledWith []string
	ReleaseLockErr        error
}

func (m *MockLockManager) AcquireLock(key, nodeID string, duration time.Duration) (chan string, error) {
	panic("implement me")
}

func (m *MockLockManager) GetLocks() (map[string]string, error) {
	panic("implement me")
}

func (m *MockLockManager) DumpLocks() error {
	panic("implement me")
}

func (m *MockLockManager) IsLocked(key string) bool {
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	if m.locks == nil {
		return false
	}

	_, exists := m.locks[key]
	return exists
}

func (m *MockLockManager) SetLock(key, nodeID string, _ time.Duration) bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	m.locks[key] = nodeID
	return true
}

func (m *MockLockManager) RenewLock(key string, durationMs int64) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	m.RenewLockCalledWith = append(m.RenewLockCalledWith, key)

	return nil
}

func (m *MockLockManager) Renew(key string, duration time.Duration) error {
	panic("implement me")
}

func (m *MockLockManager) ReleaseLock(key string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.ReleaseLockCallCount++
	m.ReleaseLockCalledWith = append(m.ReleaseLockCalledWith, key)
	if m.ReleaseLockErr != nil {
		return m.ReleaseLockErr
	}

	return nil
}

func (m *MockLockManager) Release(key string) error {
	panic("implement me")
}

func (m *MockLockManager) PendingLock(key string) (chan string, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	m.PendingLockCalledWith = append(m.PendingLockCalledWith, key)
	m.PendingLockCallCount++

	return m.PendingLockResponse, m.PendingLockResponseExists
}

func (m *MockLockManager) DeletePendingLock(key string) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

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

	Mu sync.Mutex
}

func (m *MockClusterManager) GetMembers() ([]*discovery.Member, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.GetMembersCallCount++
	if m.GetMembersErr != nil {
		return nil, m.GetMembersErr
	}

	return m.GetMembersResponse, nil
}

func (m *MockClusterManager) GetMembersCount() (int, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

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
	m.Mu.Lock()
	defer m.Mu.Unlock()

	return m.NodeID
}

func (m *MockClusterManager) BroadcastEvent(event string, data []byte) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.BroadcastEventCalledWith = append(m.BroadcastEventCalledWith, BroadcastEventInput{
		EventName: event,
		Data:      data,
	})

	return m.BroadcastEventErr
}

func (m *MockClusterManager) RegisterEventHandler(handler func(*discovery.ClusterEvent)) error {
	return nil
}
