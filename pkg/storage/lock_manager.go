package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
)

type LockManager interface {
	AcquireLock(key, nodeID string, duration time.Duration) (chan string, error)
	GetLocks() (map[string]string, error)
	IsLocked(key string) bool
	SetLock(key, nodeID string) bool
	RenewLock(key string, durationMs int64) error
	Renew(key string, duration time.Duration) error
	Release(key string) error
	ReleaseLock(key string) error
	PendingLock(key string) (chan string, bool)
	DeletePendingLock(key string)
}

type LocalLockManager struct {
	lockRespsWaiting    map[string]chan string
	lockMap             *TTLLockMap
	clusterManager      discovery.ClusterManager
	defaultLockDuration time.Duration //TODO make this come directly as Set parameter

	mu sync.RWMutex
}

func NewLocalLockManager(clusterManager discovery.ClusterManager, lockDuration time.Duration) *LocalLockManager {
	return &LocalLockManager{
		lockRespsWaiting:    make(map[string]chan string),
		lockMap:             NewTTLLockMap(),
		clusterManager:      clusterManager,
		defaultLockDuration: lockDuration,
	}
}

func (lm *LocalLockManager) AcquireLock(key, nodeID string, _ time.Duration) (chan string, error) { //TODO make duration work
	event := domain.Event{
		Key:    key,
		NodeID: nodeID,
	}

	b, err := json.Marshal(event)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	err = lm.clusterManager.BroadcastEvent(domain.ClaimKeyEventName, b)

	if err != nil {
		return nil, fmt.Errorf("failed to send user event: %w", err)
	}

	respCh := make(chan string)

	lm.lockRespsWaiting[key] = respCh
	return respCh, nil
}

func (lm *LocalLockManager) Renew(key string, duration time.Duration) error {
	if !lm.IsLocked(key) {
		return fmt.Errorf("key %s is not locked", key)
	}

	lock, err := lm.lockMap.GetLock(key)

	if err != nil {
		return fmt.Errorf("failed to get lock: %w", err)
	}

	if lock.NodeID != lm.clusterManager.GetNodeID() {
		return fmt.Errorf("lock is held by another node: %s", lock.NodeID)
	}

	lockEvent := &domain.RenewLockEvent{
		Key:        key,
		NodeID:     lock.NodeID,
		TimeMillis: duration.Milliseconds(),
	}

	b, err := json.Marshal(lockEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = lm.clusterManager.BroadcastEvent(domain.RenewLockEventName, b)

	if err != nil {
		return fmt.Errorf("failed to send user event: %w", err)
	}

	return nil
}

func (lm *LocalLockManager) Release(key string) error {
	if !lm.IsLocked(key) {
		return fmt.Errorf("key %s is not locked", key)
	}

	lock, err := lm.lockMap.GetLock(key)

	if err != nil {
		return fmt.Errorf("failed to get lock: %w", err)
	}

	if lock.NodeID != lm.clusterManager.GetNodeID() {
		return fmt.Errorf("lock is held by another node: %s", lock.NodeID)
	}

	lockEvent := &domain.ReleaseLockEvent{
		Key:    key,
		NodeID: lock.NodeID,
	}

	b, err := json.Marshal(lockEvent)

	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = lm.clusterManager.BroadcastEvent(domain.ReleaseLockEventName, b)

	if err != nil {
		return fmt.Errorf("failed to send release lock event: %w", err)
	}

	return nil
}

func (lm *LocalLockManager) IsLocked(key string) bool {
	return lm.lockMap.IsLocked(key)
}

func (lm *LocalLockManager) SetLock(key, nodeID string) bool {
	return lm.lockMap.Acquire(nodeID, key, lm.defaultLockDuration)
}

func (lm *LocalLockManager) RenewLock(key string, durationMs int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	ok := lm.lockMap.Renew(key, time.Duration(durationMs)*time.Millisecond)

	if !ok {
		return fmt.Errorf("failed to renew lock")
	}

	return nil
}

func (lm *LocalLockManager) ReleaseLock(key string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	delete(lm.lockRespsWaiting, key)

	if ok := lm.lockMap.Release(key); !ok {
		return fmt.Errorf("failed to release lock: %s", key)
	}

	return nil
}

func (lm *LocalLockManager) PendingLock(key string) (chan string, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	respCh, ok := lm.lockRespsWaiting[key]

	return respCh, ok
}

func (lm *LocalLockManager) DeletePendingLock(key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if respCh, ok := lm.lockRespsWaiting[key]; ok {
		close(respCh)
	}

	delete(lm.lockRespsWaiting, key)
}

func (lm *LocalLockManager) GetLocks() (map[string]string, error) {
	locks := lm.lockMap.Locks()

	if locks == nil {
		return nil, fmt.Errorf("failed to get locks")
	}

	locksMap := make(map[string]string)

	for _, lock := range locks {
		locksMap[lock.Key] = lock.NodeID
	}

	return locksMap, nil
}
