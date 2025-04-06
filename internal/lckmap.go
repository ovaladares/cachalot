package internal

import (
	"sync"
	"time"
)

type TTLLockMap struct {
	locks sync.Map
	mu    sync.RWMutex
}

type Lock struct {
	timer     *time.Timer
	createdAt time.Time
	duration  time.Duration
	Key       string `json:"key"`
	NodeID    string `json:"node-id"`
}

func NewTTLLockMap() *TTLLockMap {
	return &TTLLockMap{}
}

func (m *TTLLockMap) Renew(key string, duration time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if val, exists := m.locks.Load(key); exists {
		lockItem := val.(*Lock)
		lockItem.timer.Stop()
		lockItem.timer = time.AfterFunc(duration, func() {
			m.locks.Delete(key)
		})
		lockItem.duration = duration

		return true
	}

	return false
}

func (m *TTLLockMap) Acquire(node, key string, duration time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingVal, exists := m.locks.Load(key); exists {
		existingLock := existingVal.(*Lock)
		if time.Since(existingLock.createdAt) < existingLock.duration {
			return false
		}

		return false
	}

	timer := time.AfterFunc(duration, func() {
		m.locks.Delete(key)
	})

	m.locks.Store(key, &Lock{
		Key:       key,
		NodeID:    node,
		timer:     timer,
		duration:  duration,
		createdAt: time.Now(),
	})

	return true
}

func (m *TTLLockMap) Release(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if val, exists := m.locks.Load(key); exists {
		lockItem := val.(*Lock)
		lockItem.timer.Stop()
		m.locks.Delete(key)

		return true
	}

	return false
}

func (m *TTLLockMap) IsLocked(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.locks.Load(key)
	if !exists {
		return false
	}

	lockItem := val.(*Lock)
	return time.Since(lockItem.createdAt) < lockItem.duration
}

func (m *TTLLockMap) Locks() []Lock {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var locks []Lock
	m.locks.Range(func(_, value interface{}) bool {
		lockItem := value.(*Lock)
		locks = append(locks, *lockItem)
		return true
	})

	return locks
}
