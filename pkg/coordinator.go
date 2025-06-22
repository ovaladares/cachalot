package cachalot

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/ovaladares/cachalot/pkg/discovery"
	"github.com/ovaladares/cachalot/pkg/election"
	"github.com/ovaladares/cachalot/pkg/snapshot"
	"github.com/ovaladares/cachalot/pkg/storage"
)

// Coordinator defines the interface for distributed coordination operations
// such as acquiring, renewing, and releasing distributed locks across a cluster.
type Coordinator interface {
	// Connect establishes a connection to the coordination system and initializes
	// all necessary components for distributed coordination.
	// Returns an error if the connection fails.
	Connect() error

	// GetNodeID returns the unique identifier string for this node in the cluster.
	// This ID is used to identify lock ownership and node status.
	GetNodeID() string

	// Lock attempts to acquire a distributed lock on the specified key.
	// Parameters:
	//   - key: The unique identifier for the resource to lock
	//   - duration: How long the lock should be valid before expiring
	// Returns an error if the lock cannot be acquired (already locked, timeout, etc.)
	Lock(key string, duration time.Duration) error

	// Renew extends the validity period of an existing lock.
	// Parameters:
	//   - key: The identifier of the lock to renew
	//   - duration: The new duration for which the lock should be valid
	// Returns an error if the lock cannot be renewed (expired, owned by another node, etc.)
	Renew(key string, duration time.Duration) error

	// Release explicitly releases a lock held by this node.
	// Parameter:
	//   - key: The identifier of the lock to release
	// Returns an error if the lock cannot be released (not owned by this node, etc.)
	Release(key string) error

	// GetLocks retrieves a map of all active locks in the cluster.
	// Returns:
	//   - A map where keys are lock names and values are the node IDs that hold the locks
	//   - An error if the lock information cannot be retrieved
	GetLocks() (map[string]string, error)
}

// CoordinatorConfig contains all configuration parameters for a Coordinator instance.
type CoordinatorConfig struct {
	// DiscoveryProvider specifies which service discovery mechanism to use for node
	// discovery in the cluster. Currently supported: "serf"
	DiscoveryProvider string

	// ElectionConfig contains configuration parameters for leader election in the cluster.
	// This controls how leader election processes work when nodes join or leave.
	ElectionConfig *ElectionConfig

	// SnapshotConfig contains configuration parameters for snapshot management.
	SnapshotConfig *SnapshotConfig
}

// LocalCoordinator implements the Coordinator interface using local storage
// and a discovery mechanism for cluster awareness.
type LocalCoordinator struct {
	// logg is the structured logger instance for this coordinator
	logg *slog.Logger

	// bindAddr is the local address to which the coordinator binds for network communication
	bindAddr string

	// seedNodes is a list of initial nodes to contact when joining the cluster
	// if no seed nodes are provided, the coordinator will start a new cluster
	// and become the first node or if the seed nodes are not reachable
	seedNodes []string

	// lockManager handles the actual lock acquisition and management
	lockManager storage.LockManager

	// clusterManager handles node discovery and cluster membership
	clusterManager discovery.ClusterManager

	// conf contains the configuration parameters for this coordinator
	conf *CoordinatorConfig
}

// NewLocalCoordinator creates and initializes a new LocalCoordinator instance.
// Parameters:
//   - logg: A structured logger for recording operations and errors
//   - bindAddr: The network address to bind to for cluster communication
//   - seedNodes: A list of initial cluster nodes to connect with
//   - conf: Configuration parameters for the coordinator
//
// Returns a fully initialized LocalCoordinator instance.
// Panics if the discovery provider cannot be created.
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

// Connect initializes the connection to the cluster and sets up event handlers.
// This method:
//  1. Connects to the cluster using the cluster manager
//  2. Sets up an election manager for leader election
//  3. Registers event handlers for cluster membership changes
//
// Returns an error if the connection to the cluster manager fails.
func (c *LocalCoordinator) Connect() error { //Maybe return a Node instance?
	err := c.clusterManager.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to cluster manager: %w", err)
	}
	c.logg.Debug("Cluster manager connected", "node_id", c.clusterManager.GetNodeID())

	electionManager := NewElectionManager(c.GetNodeID(), c.logg, c.lockManager, c.clusterManager, election.NewStateManager(), c.conf.ElectionConfig)
	snapShotManager := NewLocalSnapshotManager(c.GetNodeID(), c.lockManager, c.clusterManager, snapshot.NewStateManager(), c.conf.SnapshotConfig, c.logg)

	eventHandler := NewServiceDiscoveryEventHandler(c.lockManager, electionManager, snapShotManager, c.GetNodeID(), c.logg)

	c.clusterManager.RegisterEventHandler(eventHandler.Handle)

	err = snapShotManager.SyncLocks()
	if err != nil {
		c.logg.Warn("Failed to sync locks from snapshot", "error", err)
	}
	c.logg.Debug("Locks synced", "node_id", c.GetNodeID())

	return nil
}

// GetNodeID returns the unique identifier of this node within the cluster.
// This ID is used to identify lock ownership and is provided by the cluster manager.
func (c *LocalCoordinator) GetNodeID() string {
	return c.clusterManager.GetNodeID()
}

// Lock attempts to acquire a distributed lock on the specified key.
// Parameters:
//   - key: The unique resource identifier to lock
//   - duration: How long the lock should be valid before expiring
//
// Returns an error if:
//   - The lock acquisition fails
//   - The lock is already held by another node
//   - The lock acquisition times out
//
// The method uses a channel to receive lock acquisition results and times out
// based on the DefaultLockDuration configuration.
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
	case <-time.After(duration):
		return fmt.Errorf("lock acquisition timed out")
	}

	return nil
}

// Renew extends the duration of an existing lock.
// Parameters:
//   - key: The identifier of the lock to renew
//   - duration: The new duration for which the lock should be valid
//
// Returns an error if the lock cannot be renewed, which might occur if:
//   - The lock doesn't exist
//   - The lock is owned by another node
//   - The lock has already expired
//   - Unknown error occurred
//
// Successfully renewed locks will have their expiration reset to the current time + duration.
// Warning: The renew time could be different for each node in the cluster due to network latency
// during the renewal process.
func (c *LocalCoordinator) Renew(key string, duration time.Duration) error {
	err := c.lockManager.Renew(key, duration)

	if err != nil {
		return fmt.Errorf("failed to renew lock: %w", err)
	}

	c.logg.Debug("Lock renewed", "key", key, "node_id", c.GetNodeID())
	return nil
}

// Release explicitly releases a lock held by this node.
// Parameter:
//   - key: The identifier of the lock to release
//
// Returns an error if the lock cannot be released, which might occur if:
//   - The lock doesn't exist
//   - The lock is owned by another node
//
// Successfully released locks become immediately available for acquisition by other nodes.
// Warning: The time to release on all nodes in the cluster may vary due to network latency
func (c *LocalCoordinator) Release(key string) error {
	err := c.lockManager.Release(key)

	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	c.logg.Debug("Lock released", "key", key, "node_id", c.GetNodeID())
	return nil
}

// GetLocks returns a map of all active locks in the cluster.
// Returns:
//   - A map where keys are lock names and values are the node IDs that hold the locks
//   - An error if the lock information cannot be retrieved
//
// This provides a snapshot of the current lock state across the cluster.
func (c *LocalCoordinator) GetLocks() (map[string]string, error) {
	locks, _ := c.lockManager.GetLocks()

	if locks == nil {
		return nil, fmt.Errorf("failed to get locks")
	}

	return locks, nil
}

// discoveryProviderFactory creates a ClusterManager based on the specified provider type.
// Parameters:
//   - provider: The type of discovery provider to create (e.g., "serf")
//   - bindAddr: The network address to bind to for cluster communication
//   - seedNodes: A list of initial cluster nodes to connect with
//   - logg: A structured logger for recording operations and errors
//
// Returns:
//   - A configured ClusterManager implementation
//   - An error if the requested provider cannot be created
//
// Currently supports "serf" as a provider, with serf as the default if not specified.
func discoveryProviderFactory(provider string, bindAddr string, seedNodes []string, logg *slog.Logger) (discovery.ClusterManager, error) {
	switch provider {
	case "serf":
		return discovery.NewSerfDiscover(bindAddr, seedNodes, logg), nil
	default:
		return discovery.NewSerfDiscover(bindAddr, seedNodes, logg), nil
	}
}
