package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
)

var DefaultLockDuration = 120 * time.Second

type Coordinator interface {
	Connect() error
	GetNodeID() string
	Lock(key string) error
	GetLocks() (map[string]string, error)
}

type LocalCoordinator struct {
	logg        *slog.Logger
	bindAddr    string
	seedNodes   []string
	nodeID      string
	lockManager LockManager
	serf        *serf.Serf
}

func NewLocalCoordinator(logg *slog.Logger, bindAddr string, seedNodes []string) *LocalCoordinator {
	return &LocalCoordinator{
		logg:      logg,
		bindAddr:  bindAddr,
		seedNodes: seedNodes,
		nodeID:    fmt.Sprintf("node-%d", rand.IntN(10000)), //TODO use a better way to generate node ID
	}
}

func (c *LocalCoordinator) Connect() error { //Maybe return a Node instance?
	c.logg.Debug("Connecting to local coordinator")

	addr, err := net.ResolveTCPAddr("tcp", c.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()

	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	serfConfig.LogOutput = io.Discard                  //TODO optional parameter
	serfConfig.MemberlistConfig.LogOutput = io.Discard //TODO optional parameter surpress serf logs

	currentNodeID := c.GetNodeID()

	eventsChan := make(chan serf.Event, 256)

	serfConfig.EventCh = eventsChan
	serfConfig.EnableNameConflictResolution = false
	serfConfig.NodeName = currentNodeID

	serfInstance, err := serf.Create(serfConfig)
	if err != nil {
		return fmt.Errorf("failed to create serf instance for service discovery: %w", err)
	}

	c.logg.Debug("Serf instance created", "node_id", currentNodeID)

	joined, err := serfInstance.Join(c.seedNodes, true) //Need to validate ignore old

	if err != nil {
		c.logg.Warn("Failed to join cluster, running as standalone", "node_id", currentNodeID, "error", err)
	}

	c.logg.Info("Joined cluster", "node_id", currentNodeID, "known_nodes", joined)

	lckManager := NewLocalLockManager(serfInstance)
	electionManager := NewElectionManager(currentNodeID, c.logg, serfInstance, lckManager)

	worker := NewEventsWorker(lckManager, electionManager, currentNodeID, eventsChan, c.logg, serfInstance)

	go worker.Start(context.Background()) //TODO add context

	c.serf = serfInstance
	c.lockManager = lckManager

	return nil
}

func (c *LocalCoordinator) GetNodeID() string {
	return c.nodeID
}

func (c *LocalCoordinator) Lock(key string) error {
	ch, err := c.lockManager.AcquireLock(key, c.nodeID, DefaultLockDuration)

	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	select {
	case nodeID := <-ch:
		if nodeID != c.nodeID {
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
