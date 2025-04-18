package internal

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/otaviovaladares/cachalot/internal/discovery"
)

var DefaultLockDuration = 120 * time.Second

type Coordinator interface {
	Connect() error
	GetNodeID() string
	Lock(key string) error
	GetLocks() (map[string]string, error)
}

type LocalCoordinator struct {
	logg           *slog.Logger
	bindAddr       string
	seedNodes      []string
	lockManager    LockManager
	clusterManager discovery.ClusterManager
}

func NewLocalCoordinator(logg *slog.Logger, bindAddr string, seedNodes []string) *LocalCoordinator {
	discovery := discovery.NewSerfDiscover(bindAddr, seedNodes, logg)

	return &LocalCoordinator{
		logg:           logg,
		bindAddr:       bindAddr,
		seedNodes:      seedNodes,
		clusterManager: discovery,
		lockManager:    NewLocalLockManager(discovery),
	}
}

func (c *LocalCoordinator) Connect() error { //Maybe return a Node instance?
	err := c.clusterManager.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to cluster manager: %w", err)
	}
	c.logg.Debug("Cluster manager connected", "node_id", c.clusterManager.GetNodeID())

	electionManager := NewElectionManager(c.GetNodeID(), c.logg, c.lockManager, c.clusterManager)
	eventHandler := NewServiceDiscoveryEventHandler(c.lockManager, electionManager, c.GetNodeID(), c.logg)

	c.clusterManager.RegisterEventHandler(eventHandler.Handle)

	return nil
}

func (c *LocalCoordinator) GetNodeID() string {
	return c.clusterManager.GetNodeID()
}

func (c *LocalCoordinator) Lock(key string) error {
	ch, err := c.lockManager.AcquireLock(key, c.GetNodeID(), DefaultLockDuration)

	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	select {
	case nodeID := <-ch:
		if nodeID != c.GetNodeID() {
			return fmt.Errorf("lock already acquired by another node")
		}

		c.logg.Debug("Lock acquired", "key", key, "node_id", nodeID)
	case <-time.After(DefaultLockDuration):
		return fmt.Errorf("lock acquisition timed out")
	}

	return nil
}

func (c *LocalCoordinator) GetLocks() (map[string]string, error) {
	locks, _ := c.lockManager.GetLocks()

	if locks == nil {
		return nil, fmt.Errorf("failed to get locks")
	}

	return locks, nil
}
