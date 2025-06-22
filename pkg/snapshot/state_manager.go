package snapshot

import (
	"sync"

	"github.com/ovaladares/cachalot/pkg/domain"
)

type StateManager struct {
	snapshots map[string]*domain.LocksSnapShotEvent
	count     int
	mu        sync.RWMutex
}

func NewStateManager() *StateManager {
	return &StateManager{
		snapshots: make(map[string]*domain.LocksSnapShotEvent),
		count:     0,
	}
}

func (s *StateManager) AddSnapshot(nodeID string, lock map[string]domain.LockSnapshot) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++

	s.snapshots[nodeID] = &domain.LocksSnapShotEvent{
		NodeID: nodeID,
		Locks:  lock,
	}

	return s.count
}

func (s *StateManager) GetAllSnapshots() map[string]*domain.LocksSnapShotEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshotsCopy := make(map[string]*domain.LocksSnapShotEvent, len(s.snapshots))
	for k, v := range s.snapshots {
		snapshotsCopy[k] = v
	}

	return snapshotsCopy
}

func (s *StateManager) ClearSnapshots() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshots = make(map[string]*domain.LocksSnapShotEvent)
	s.count = 0
}
