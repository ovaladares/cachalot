package storage_test

import (
	"testing"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestTTLLockMap_AcquireSucess(t *testing.T) {
	m := storage.NewTTLLockMap()

	node := "node1"
	key := "value1"
	duration := 1 * time.Second

	ok := m.Acquire(node, key, duration)
	assert.True(t, ok)

	assert.True(t, m.IsLocked(key))

	time.Sleep(duration + 30*time.Millisecond)

	assert.False(t, m.IsLocked(key))
}

// func TestTTLLockMap_AcquireFailAlreadyExists(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	node := "node1"
// 	key := "value1"
// 	duration := 1 * time.Second

// 	ok := m.Acquire(node, key, duration)
// 	assert.True(t, ok)

// 	ok = m.Acquire(node, key, duration)
// 	assert.False(t, ok)

// 	assert.True(t, m.IsLocked(key))

// 	time.Sleep(duration + 30*time.Millisecond)

// 	assert.False(t, m.IsLocked(key))
// }

// func TestTTLLockMap_RenewSuccess(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	node := "node1"
// 	key := "value1"
// 	duration := 500 * time.Millisecond

// 	ok := m.Acquire(node, key, duration)
// 	assert.True(t, ok)

// 	time.Sleep(200 * time.Millisecond)

// 	renewed := m.Renew(key, 1*time.Second)
// 	assert.True(t, renewed)

// 	assert.True(t, m.IsLocked(key))

// 	time.Sleep(350 * time.Millisecond)

// 	assert.True(t, m.IsLocked(key))
// }

// func TestTTLLockMap_RenewDontExists(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	duration := 1 * time.Second

// 	renewed := m.Renew("non-existing-key", duration*4)
// 	assert.False(t, renewed)
// }

// func TestTTLLockMap_RenewLockSchedule(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	node := "node1"
// 	key := "value1"
// 	duration := 1 * time.Second

// 	ok := m.Acquire(node, key, duration)
// 	assert.True(t, ok)

// 	renewed := m.Renew(key, duration*2)
// 	assert.True(t, renewed)

// 	time.Sleep(duration + 30*time.Millisecond)

// 	assert.True(t, m.IsLocked(key))

// 	time.Sleep(duration*2 + 30*time.Millisecond)
// 	assert.False(t, m.IsLocked(key))
// }

// func TestTTLLockMap_ReleaseSuccess(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	node := "node1"
// 	key := "value1"
// 	duration := 60 * time.Second

// 	ok := m.Acquire(node, key, duration)
// 	assert.True(t, ok)

// 	released := m.Release(key)
// 	assert.True(t, released)

// 	assert.False(t, m.IsLocked(key))
// }

// func TestTTLLockMap_ReleaseDontExists(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	released := m.Release("non-existing-key")
// 	assert.False(t, released)
// }

// func TestTTLLockMap_Locks(t *testing.T) {
// 	m := storage.NewTTLLockMap()

// 	node := "node1"
// 	key := "value1"
// 	node2 := "node2"
// 	key2 := "value2"
// 	duration := 120 * time.Second

// 	ok := m.Acquire(node, key, duration)
// 	assert.True(t, ok)

// 	ok = m.Acquire(node2, key2, duration)
// 	assert.True(t, ok)

// 	locks := m.Locks()
// 	assert.Len(t, locks, 2)
// 	assert.Equal(t, key, locks[0].Key)
// 	assert.Equal(t, node, locks[0].NodeID)

// 	assert.Equal(t, key2, locks[1].Key)
// 	assert.Equal(t, node2, locks[1].NodeID)
// }
