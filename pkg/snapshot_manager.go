package cachalot

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/snapshot"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

type SnapshotManager interface {
	SyncLocks() error

	AddSnapshot(event *domain.LocksSnapShotEvent) error
}

type LocalSnapshotManager struct {
	nodeName       string
	lockManager    storage.LockManager
	clusterManager discovery.ClusterManager
	stateManager   *snapshot.StateManager
	logg           *slog.Logger
}

func NewLocalSnapshotManager(
	nodeName string,
	lockManager storage.LockManager,
	clusterManager discovery.ClusterManager,
	stateManager *snapshot.StateManager,
	logg *slog.Logger,
) *LocalSnapshotManager {
	return &LocalSnapshotManager{
		nodeName:       nodeName,
		lockManager:    lockManager,
		clusterManager: clusterManager,
		stateManager:   stateManager,
		logg:           logg,
	}
}

func (m *LocalSnapshotManager) AddSnapshot(event *domain.LocksSnapShotEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	if event.NodeID == m.nodeName {
		m.logg.Debug("Ignoring self locks snapshot event")
		return nil
	}

	m.logg.Debug("Adding locks snapshot", "node-id", event.NodeID)

	m.stateManager.AddSnapshot(event.NodeID, event.Locks)

	return nil
}

func (m *LocalSnapshotManager) SyncLocks() error {
	locksReuquestEvent := domain.LocksRequestEvent{
		NodeID: m.nodeName,
	}

	b, err := json.Marshal(locksReuquestEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = m.clusterManager.BroadcastEvent(domain.LocksRequestEventName, b)
	if err != nil {
		return fmt.Errorf("failed to send user event: %w", err)
	}

	// Wait for the locks snapshot event
	time.Sleep(3 * time.Second) // TODO: replace with configurable timeout

	snapshots := m.stateManager.GetAllSnapshots()
	if len(snapshots) == 0 {
		return fmt.Errorf("no snapshots received")
	}

	m.logg.Debug("Received locks snapshots", "len", len(snapshots))

	var snapshotWinner *domain.LocksSnapShotEvent

	for _, snapshot := range snapshots {
		if snapshot.NodeID == m.nodeName {
			m.logg.Debug("Ignoring self locks snapshot event")
			continue
		}

		if snapshotWinner == nil {
			snapshotWinner = snapshot
			continue
		}

		nodeNumber, err := strconv.Atoi(strings.Split(snapshot.NodeID, "-")[1])
		if err != nil {
			return fmt.Errorf("failed to parse node number: %w", err)
		}

		winnerNodeNumber, err := strconv.Atoi(strings.Split(snapshotWinner.NodeID, "-")[1])
		if err != nil {
			return fmt.Errorf("failed to parse winner node number: %w", err)
		}

		if winnerNodeNumber > nodeNumber {
			snapshotWinner = snapshot
		}
	}

	for key, lock := range snapshotWinner.Locks {
		if lock.NodeID == m.nodeName {
			continue
		}

		m.logg.Debug("Acquiring lock", "key", key, "node-id", lock.NodeID)

		ok := m.lockManager.SetLock(key, lock.NodeID, time.Duration(lock.TimeMillis)*time.Millisecond)
		if !ok {
			return fmt.Errorf("failed to acquire lock for key: %s", key)
		}
	}

	m.stateManager.ClearSnapshots()

	m.logg.Debug("Locks synced successfully", "node-id", m.nodeName)
	return nil
}
