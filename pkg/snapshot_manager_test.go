package cachalot_test

import (
	"log/slog"
	"testing"
	"time"

	cachalot "github.com/otaviovaladares/cachalot/pkg"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/snapshot"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotManager_AddSnapshot(t *testing.T) {
	conf := &cachalot.SnapshotConfig{
		TimeToWaitForSnapshot: 3 * time.Second,
	}

	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}
	stateManager := snapshot.NewStateManager()
	logg := slog.Default()

	snapshotManager := cachalot.NewLocalSnapshotManager(
		nodeName,
		lockManager,
		clusterManager,
		stateManager,
		conf,
		logg,
	)

	lockSnap := domain.LockSnapshot{
		Key:        "lock1",
		NodeID:     "node2",
		TimeMillis: 1234567890,
	}

	event := &domain.LocksSnapShotEvent{
		NodeID: "node2",
		Locks: map[string]domain.LockSnapshot{
			"lock1": lockSnap,
		},
	}

	err := snapshotManager.AddSnapshot(event)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(stateManager.GetAllSnapshots()))
	assert.Equal(t, event, stateManager.GetAllSnapshots()["node2"])
}

func TestSnapshotManager_AddSnapshotSameNodeID(t *testing.T) {
	conf := &cachalot.SnapshotConfig{
		TimeToWaitForSnapshot: 3 * time.Second,
	}

	nodeName := "node2"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}
	stateManager := snapshot.NewStateManager()
	logg := slog.Default()

	snapshotManager := cachalot.NewLocalSnapshotManager(
		nodeName,
		lockManager,
		clusterManager,
		stateManager,
		conf,
		logg,
	)

	lockSnap := domain.LockSnapshot{
		Key:        "lock1",
		NodeID:     "node2",
		TimeMillis: 1234567890,
	}

	event := &domain.LocksSnapShotEvent{
		NodeID: "node2",
		Locks: map[string]domain.LockSnapshot{
			"lock1": lockSnap,
		},
	}

	err := snapshotManager.AddSnapshot(event)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(stateManager.GetAllSnapshots()))
}

func TestSnapshotManger_SyncLocksSuccessVariosLocks(t *testing.T) {
	conf := &cachalot.SnapshotConfig{
		TimeToWaitForSnapshot: 3 * time.Second,
	}

	nodeName := "node-1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}
	stateManager := snapshot.NewStateManager()
	logg := slog.Default()

	snapshotManager := cachalot.NewLocalSnapshotManager(
		nodeName,
		lockManager,
		clusterManager,
		stateManager,
		conf,
		logg,
	)

	expectedLocks := map[string]string{
		"lock1": "node-2",
	}

	lockSnap1 := domain.LockSnapshot{
		Key:        "lock1",
		NodeID:     "node-2",
		TimeMillis: 1234567890,
	}

	lockSnap2 := domain.LockSnapshot{
		Key:        "lock2",
		NodeID:     "node-3",
		TimeMillis: 1234567891,
	}

	event1 := &domain.LocksSnapShotEvent{
		NodeID: "node-2",
		Locks: map[string]domain.LockSnapshot{
			"lock1": lockSnap1,
		},
	}

	event2 := &domain.LocksSnapShotEvent{
		NodeID: "node-3",
		Locks: map[string]domain.LockSnapshot{
			"lock2": lockSnap2,
		},
	}

	err := snapshotManager.AddSnapshot(event1)
	assert.NoError(t, err)

	err = snapshotManager.AddSnapshot(event2)
	assert.NoError(t, err)

	snapshotManager.SyncLocks()
	time.Sleep(4 * time.Second) // Wait for the snapshots to be processed

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 1)
	assert.Equal(t, domain.LocksRequestEventName, clusterManager.BroadcastEventCalledWith[0].EventName, "Broadcasted right event to sync locks")

	lcks, _ := lockManager.GetLocks()
	assert.Equal(t, expectedLocks, lcks, "Locks should match the snapshot of smallest node ID")
	assert.Equal(t, 0, len(stateManager.GetAllSnapshots()), "State manager should have cleaned its storage")
}

func TestSnapshotManger_SyncLocksSuccessNoLocksReceived(t *testing.T) {
	conf := &cachalot.SnapshotConfig{
		TimeToWaitForSnapshot: 3 * time.Second,
	}
	nodeName := "node-1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}
	stateManager := snapshot.NewStateManager()
	logg := slog.Default()

	snapshotManager := cachalot.NewLocalSnapshotManager(
		nodeName,
		lockManager,
		clusterManager,
		stateManager,
		conf,
		logg,
	)

	snapshotManager.SyncLocks()
	time.Sleep(4 * time.Second) // Wait for the snapshots to be processed

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 1)
	assert.Equal(t, domain.LocksRequestEventName, clusterManager.BroadcastEventCalledWith[0].EventName, "Broadcasted right event to sync locks")

	lcks, _ := lockManager.GetLocks()

	assert.Equal(t, 0, len(lcks), "No locks should be present as no snapshots were received")
	assert.Equal(t, 0, len(stateManager.GetAllSnapshots()), "State manager should have cleaned its storage")
}
