package internal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
)

type LockManager interface {
	AcquireLock(key, nodeID string, duration time.Duration) (chan string, error)
	GetLocks() (map[string]string, error)
	IsLocked(key string) bool
	SetLock(key, nodeID string) bool
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
	event := Event{
		Key:    key,
		NodeID: nodeID,
	}

	b, err := json.Marshal(event)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	err = lm.clusterManager.BroadcastEvent(ClaimKeyEventName, b)

	if err != nil {
		return nil, fmt.Errorf("failed to send user event: %w", err)
	}

	respCh := make(chan string)

	lm.lockRespsWaiting[key] = respCh
	return respCh, nil
}

func (lm *LocalLockManager) IsLocked(key string) bool {
	return lm.lockMap.IsLocked(key)
}

func (lm *LocalLockManager) SetLock(key, nodeID string) bool {
	return lm.lockMap.Acquire(nodeID, key, lm.defaultLockDuration)
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
