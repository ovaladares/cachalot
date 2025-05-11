package cachalot

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/election"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

type Coordinator interface {
	Connect() error
	GetNodeID() string
	Lock(key string, duration time.Duration) error
	Renew(key string, duration time.Duration) error
	Release(key string) error
	GetLocks() (map[string]string, error)
}

type CoordinatorConfig struct {
	DefaultLockDuration time.Duration
	DiscoveryProvider   string
	ElectionConfig      *ElectionConfig
}

type LocalCoordinator struct {
	logg           *slog.Logger
	bindAddr       string
	seedNodes      []string
	lockManager    storage.LockManager
	clusterManager discovery.ClusterManager
	conf           *CoordinatorConfig
}

func NewLocalCoordinator(logg *slog.Logger, bindAddr string, seedNodes []string, conf *CoordinatorConfig) *LocalCoordinator {
	discovery, err := discoveryProviderFactory(conf.DiscoveryProvider, bindAddr, seedNodes, logg)

	if err != nil {
		logg.Error("failed to create discovery provider", "error", err)

		panic(err)
	}

	return &LocalCoordinator{
		logg:           logg,
		bindAddr:       bindAddr,
		seedNodes:      seedNodes,
		clusterManager: discovery,
		lockManager:    storage.NewLocalLockManager(discovery),
		conf:           conf,
	}
}

func (c *LocalCoordinator) Connect() error { //Maybe return a Node instance?
	err := c.clusterManager.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to cluster manager: %w", err)
	}
	c.logg.Debug("Cluster manager connected", "node_id", c.clusterManager.GetNodeID())

	electionManager := NewElectionManager(c.GetNodeID(), c.logg, c.lockManager, c.clusterManager, election.NewStateManager(), c.conf.ElectionConfig)
	eventHandler := NewServiceDiscoveryEventHandler(c.lockManager, electionManager, c.GetNodeID(), c.logg)

	c.clusterManager.RegisterEventHandler(eventHandler.Handle)

	return nil
}

func (c *LocalCoordinator) GetNodeID() string {
	return c.clusterManager.GetNodeID()
}

func (c *LocalCoordinator) Lock(key string, duration time.Duration) error {
	ch, err := c.lockManager.AcquireLock(key, c.GetNodeID(), duration)

	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	select {
	case nodeID, ok := <-ch:
		if !ok {
			return fmt.Errorf("Lock acquired by another node")
		}

		if nodeID != c.GetNodeID() {
			return fmt.Errorf("lock already acquired by another node")
		}

		c.logg.Debug("Lock acquired", "key", key, "node_id", nodeID)
	case <-time.After(c.conf.DefaultLockDuration):
		return fmt.Errorf("lock acquisition timed out")
	}

	return nil
}

func (c *LocalCoordinator) Renew(key string, duration time.Duration) error {
	err := c.lockManager.Renew(key, duration)

	if err != nil {
		return fmt.Errorf("failed to renew lock: %w", err)
	}

	c.logg.Debug("Lock renewed", "key", key, "node_id", c.GetNodeID())
	return nil
}

func (c *LocalCoordinator) Release(key string) error {
	err := c.lockManager.Release(key)

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	c.logg.Debug("Lock released", "key", key, "node_id", c.GetNodeID())
	return nil
}

func (c *LocalCoordinator) GetLocks() (map[string]string, error) {
	locks, _ := c.lockManager.GetLocks()

	if locks == nil {
		return nil, fmt.Errorf("failed to get locks")
	}

	return locks, nil
}

func discoveryProviderFactory(provider string, bindAddr string, seedNodes []string, logg *slog.Logger) (discovery.ClusterManager, error) {
	switch provider {
	case "serf":
		return discovery.NewSerfDiscover(bindAddr, seedNodes, logg), nil
	default:
		return discovery.NewSerfDiscover(bindAddr, seedNodes, logg), nil
	}
}
