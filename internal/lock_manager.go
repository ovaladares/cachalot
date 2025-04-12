package internal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
)

type LockManager interface {
	AcquireLock(key, nodeID string, duration time.Duration) (chan string, error)
	isLocked(key string) bool
	setLock(key, nodeID string) bool
	pendingLock(key string) (chan string, bool)
	deletePendingLock(key string)
}

type LocalLockManager struct {
	lockRespsWaiting map[string]chan string
	lockMap          *TTLLockMap
	serf             *serf.Serf

	mu sync.RWMutex
}

func NewLocalLockManager(serf *serf.Serf) *LocalLockManager {
	return &LocalLockManager{
		lockRespsWaiting: make(map[string]chan string),
		lockMap:          NewTTLLockMap(),
		serf:             serf,
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

	respCh := make(chan string)

	lm.lockRespsWaiting[key] = respCh

	err = lm.serf.UserEvent(claimKeyEventName, b, false)

	if err != nil {
		return nil, fmt.Errorf("failed to send user event: %w", err)
	}

	return respCh, nil
}

func (lm *LocalLockManager) isLocked(key string) bool {
	return lm.lockMap.IsLocked(key)
}

func (lm *LocalLockManager) setLock(key, nodeID string) bool {
	return lm.lockMap.Acquire(nodeID, key, DefaultLockDuration)
}

func (lm *LocalLockManager) pendingLock(key string) (chan string, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	respCh, ok := lm.lockRespsWaiting[key]

	return respCh, ok
}

func (lm *LocalLockManager) deletePendingLock(key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, ok := lm.lockRespsWaiting[key]; ok {
		delete(lm.lockRespsWaiting, key)
	}
}
