package storage

import (
	"fmt"
	"sync"
	"time"
)

type TTLLockMap struct {
	locks map[string]*Lock
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
	return &TTLLockMap{
		locks: make(map[string]*Lock),
	}
}

func (m *TTLLockMap) Renew(key string, duration time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lockItem, exists := m.locks[key]; exists {
		lockItem.timer.Stop()
		delete(m.locks, key)

		timer := time.AfterFunc(duration, func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			delete(m.locks, key)
		})

		m.locks[key] = &Lock{
			Key:       key,
			NodeID:    lockItem.NodeID,
			timer:     timer,
			duration:  duration,
			createdAt: lockItem.createdAt,
		}

		return true
	}

	return false
}

func (m *TTLLockMap) Acquire(node, key string, duration time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.locks[key]; exists {
		return false
	}

	timer := time.AfterFunc(duration, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.locks, key)
	})

	m.locks[key] = &Lock{
		Key:       key,
		NodeID:    node,
		timer:     timer,
		duration:  duration,
		createdAt: time.Now(),
	}

	return true
}

func (m *TTLLockMap) Release(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lockItem, exists := m.locks[key]; exists {
		lockItem.timer.Stop()
		delete(m.locks, key)

		return true
	}

	return false
}

func (m *TTLLockMap) IsLocked(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.locks[key]
	if !exists {
		return false
	}

	return time.Since(val.createdAt) < val.duration
}

func (m *TTLLockMap) Locks() []Lock {
	m.mu.RLock()
	defer m.mu.RUnlock()

	locks := make([]Lock, 0, len(m.locks))
	for _, lockItem := range m.locks {
		locks = append(locks, *lockItem)
	}

	return locks
}

func (m *TTLLockMap) GetLock(key string) (*Lock, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lck, ok := m.locks[key]

	if !ok {
		return nil, fmt.Errorf("lock not found")
	}

	return lck, nil
}
