package internal_test

import (
	"testing"
	"time"

	"github.com/otaviovaladares/cachalot/internal"
	"github.com/otaviovaladares/cachalot/internal/discovery"
	"github.com/stretchr/testify/assert"
)

type BroadcastEventInput struct {
	EventName string
	Data      []byte
}

type MockClusterManager struct {
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
	return "node1"
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
	m := internal.NewLocalLockManager(&MockClusterManager{})

	node := "node1"
	key := "value1"

	ok := m.SetLock(key, node)
	assert.True(t, ok)

	assert.True(t, m.IsLocked(key))
}

func TestLockManagerIsLocked_NoLock(t *testing.T) {
	m := internal.NewLocalLockManager(&MockClusterManager{})

	key := "value1"

	assert.False(t, m.IsLocked(key))
}
func TestLockManagerSetLock_Success(t *testing.T) {
	m := internal.NewLocalLockManager(&MockClusterManager{})

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
	m := internal.NewLocalLockManager(&MockClusterManager{})

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
	m := internal.NewLocalLockManager(&MockClusterManager{})

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
	m := internal.NewLocalLockManager(&MockClusterManager{})

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, ch)

	respCh, ok := m.PendingLock("value1")
	assert.True(t, ok)
	assert.Equal(t, ch, respCh)
}

func TestLockManagerAcquireLock_BroadcastEventError(t *testing.T) {
	m := internal.NewLocalLockManager(&MockClusterManager{
		BroadcastEventErr: assert.AnError,
	})

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.Error(t, err)
	assert.Nil(t, ch)

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}

func TestLockManagerDeletePendingLock_Success(t *testing.T) {
	m := internal.NewLocalLockManager(&MockClusterManager{})

	ch, err := m.AcquireLock("value1", "node1", 10*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, ch)

	m.DeletePendingLock("value1")

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}

func TestLockManagerDeletePendingLock_NoPendingLock(t *testing.T) {
	m := internal.NewLocalLockManager(&MockClusterManager{})

	m.DeletePendingLock("value1")

	respCh, ok := m.PendingLock("value1")
	assert.False(t, ok)
	assert.Nil(t, respCh)
}
