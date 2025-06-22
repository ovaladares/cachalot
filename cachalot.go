package cachalot

import (
	"time"

	cachalot "github.com/ovaladares/cachalot/pkg"
)

// Coordinator provides distributed coordination capabilities
// through lock acquisition, renewal, and release operations.
// It serves as a client-facing wrapper around the internal coordinator implementation.
type Coordinator struct {
	internal cachalot.Coordinator
}

// NewCoordinator creates a new Coordinator instance configured with the provided parameters.
//
// Parameters:
//   - bindAddr: The address to bind the coordinator's networking component to (format: "host:port")
//   - seedNodes: A list of seed node addresses used for bootstrapping the distributed system
//   - config: Configuration options for the coordinator
//
// Returns:
//   - A new Coordinator instance ready to be connected
func NewCoordinator(bindAddr string, seedNodes []string, config *Config) *Coordinator {
	conf := NewConfig(config)

	conf.Logger.Debug("Creating new local coordinator")

	localCoordinator := cachalot.NewLocalCoordinator(
		conf.Logger,
		bindAddr,
		seedNodes,
		&cachalot.CoordinatorConfig{
			DiscoveryProvider: conf.DiscoveryBackend,
			ElectionConfig: &cachalot.ElectionConfig{
				TimeToWaitForVotes: conf.ElectionConfig.TimeToWaitForVotes,
			},
			SnapshotConfig: &cachalot.SnapshotConfig{
				TimeToWaitForSnapshot: conf.SnapshotConfig.TimeToWaitForSnapshot,
			},
		},
	)

	return &Coordinator{
		internal: localCoordinator,
	}
}

// Connect establishes the network connection for the coordinator
// and joins the distributed system cluster.
//
// The connection must be performed before any lock operations can be executed.
//
// Returns:
//   - error: nil if successful, otherwise an error describing what went wrong
func (c *Coordinator) Connect() error {
	err := c.internal.Connect()

	if err != nil {
		return err
	}

	return nil
}

// Lock attempts to acquire a distributed lock for the specified key.
//
// Parameters:
//   - key: The unique identifier for the resource to lock
//   - duration: How long the lock should be held before automatically expiring
//
// Returns:
//   - error: nil if the lock was successfully acquired, otherwise an error
//     describing why the lock couldn't be acquired (e.g., if the lock is already held)
//     or timeout
func (c *Coordinator) Lock(key string, duration time.Duration) error {
	err := c.internal.Lock(key, duration)

	if err != nil {
		return err
	}

	return nil
}

// Renew extends the duration of an existing lock that the coordinator already holds.
//
// Parameters:
//   - key: The identifier of the lock to renew
//   - duration: The new duration to extend the lock for
//
// Returns:
//   - error: nil if the lock was successfully renewed, otherwise an error
//     (e.g., if the lock doesn't exist or is held by another node)
func (c *Coordinator) Renew(key string, duration time.Duration) error {
	err := c.internal.Renew(key, duration)

	if err != nil {
		return err
	}

	return nil
}

// Release explicitly releases a lock held by this coordinator.
//
// Parameters:
//   - key: The identifier of the lock to release
//
// Returns:
//   - error: nil if the lock was successfully released, otherwise an error
//     (e.g., if the lock doesn't exist or is held by another node)
func (c *Coordinator) Release(key string) error {
	err := c.internal.Release(key)

	if err != nil {
		return err
	}

	return nil
}

// GetLocks retrieves information about all locks currently held in the system.
//
// Returns:
//   - map[string]string: A map of lock keys to node IDs that hold those locks
//   - error: nil if the lock information was successfully retrieved, otherwise an error
func (c *Coordinator) GetLocks() (map[string]string, error) {
	locks, err := c.internal.GetLocks()

	if err != nil {
		return nil, err
	}

	return locks, nil
}
