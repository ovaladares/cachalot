package storage_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

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

func TestLockManagerIsLocked_Success(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	assert.True(t, m.IsLocked(key))
}

func TestLockManagerIsLocked_NoLock(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	key := "value1"

	assert.False(t, m.IsLocked(key))
}
func TestLockManagerSetLock_Success(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	locks, err := m.GetLocks()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(locks))
	assert.Equal(t, node, locks[key])
}

func TestLockManagerSetLock_AlreadyLocked(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	node1 := "node1"
	node2 := "node2"
	key := "value1"

	ok := m.SetLock(key, node1)
	assert.True(t, ok)

	ok = m.SetLock(key, node2)
	assert.False(t, ok)

	locks, err := m.GetLocks()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(locks))
	assert.Equal(t, node1, locks[key])
}

func TestLockManagerGetLocks_Succes(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	node1 := "node1"
	node2 := "node2"
	key1 := "value1"
	key2 := "value2"

	ok := m.SetLock(key1, node1)
	assert.True(t, ok)

	ok = m.SetLock(key2, node2)
	assert.True(t, ok)

	locks, err := m.GetLocks()

	assert.NoError(t, err)
	assert.Equal(t, 2, len(locks))
	assert.Equal(t, node1, locks[key1])
	assert.Equal(t, node2, locks[key2])
}

func TestLockManagerAcquireLock_Sucess(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, ch)

	respCh, ok := m.PendingLock("value1")
	assert.True(t, ok)
	assert.Equal(t, ch, respCh)
}

func TestLockManagerAcquireLock_BroadcastEventError(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{
		BroadcastEventErr: assert.AnError,
	}, 10*time.Second)

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.Error(t, err)
	assert.Nil(t, ch)

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}

func TestLockManagerDeletePendingLock_Success(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, ch)

	m.DeletePendingLock("value1")

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}

func TestLockManagerDeletePendingLock_NoPendingLock(t *testing.T) {
	m := storage.NewLocalLockManager(&MockClusterManager{}, 10*time.Second)

	m.DeletePendingLock("value1")

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}

func TestLockManagerRenew_Success(t *testing.T) {
	mockClusterManager := &MockClusterManager{
		NodeID: "node1",
	}

	m := storage.NewLocalLockManager(mockClusterManager, 10*time.Second)

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	err := m.Renew(key, 20*time.Second)
	assert.NoError(t, err)

	locks, err := m.GetLocks()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(locks))
	assert.Equal(t, node, locks[key])

	renewLockEvent := &domain.RenewLockEvent{}
	err = json.Unmarshal(mockClusterManager.BroadcastEventCalledWith[0].Data, renewLockEvent)

	assert.NoError(t, err)

	assert.Equal(t, 1, len(mockClusterManager.BroadcastEventCalledWith))
	assert.Equal(t, domain.RenewLockEventName, mockClusterManager.BroadcastEventCalledWith[0].EventName)
	assert.Equal(t, key, renewLockEvent.Key)
	assert.Equal(t, 20*time.Second, time.Duration(renewLockEvent.TimeMillis)*time.Millisecond)
}

func TestLockManagerRenew_ErrorNodeIsNotOwnerOfLock(t *testing.T) {
	mockClusterManager := &MockClusterManager{
		NodeID:                   "node1",
		BroadcastEventCalledWith: []BroadcastEventInput{},
	}

	m := storage.NewLocalLockManager(mockClusterManager, 10*time.Second)

	node := "node2"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	err := m.Renew(key, 20*time.Second)
	assert.Error(t, err)

	assert.Equal(t, 0, len(mockClusterManager.BroadcastEventCalledWith))
}

func TestLockManagerRenew_ErrorNotLocked(t *testing.T) {
	mockClusterManager := &MockClusterManager{
		NodeID:                   "node1",
		BroadcastEventCalledWith: []BroadcastEventInput{},
	}

	m := storage.NewLocalLockManager(mockClusterManager, 10*time.Second)

	key := "value1"

	err := m.Renew(key, 20*time.Second)
	assert.Error(t, err)

	assert.Equal(t, 0, len(mockClusterManager.BroadcastEventCalledWith))
}

func TestLockManagerRenew_BroadcastEventErr(t *testing.T) {
	mockClusterManager := &MockClusterManager{
		NodeID:            "node1",
		BroadcastEventErr: errors.New("broadcast event error"),
	}

	m := storage.NewLocalLockManager(mockClusterManager, 10*time.Second)

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	err := m.Renew(key, 20*time.Second)
	assert.Error(t, err)

	renewLockEvent := &domain.RenewLockEvent{}
	err = json.Unmarshal(mockClusterManager.BroadcastEventCalledWith[0].Data, renewLockEvent)

	assert.NoError(t, err)

	assert.Equal(t, 1, len(mockClusterManager.BroadcastEventCalledWith))
	assert.Equal(t, domain.RenewLockEventName, mockClusterManager.BroadcastEventCalledWith[0].EventName)
	assert.Equal(t, key, renewLockEvent.Key)
	assert.Equal(t, 20*time.Second, time.Duration(renewLockEvent.TimeMillis)*time.Millisecond)
}

func TestLockManagerRenewLock_Success(t *testing.T) {
	mockClusterManager := &MockClusterManager{}

	m := storage.NewLocalLockManager(mockClusterManager, 1*time.Second)

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	err := m.RenewLock(key, 20*time.Second.Milliseconds())
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	locks, err := m.GetLocks()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(locks))
	assert.Equal(t, node, locks[key])
}
