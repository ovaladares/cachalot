package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
)

var DefaultLockDuration = 120 * time.Second

type LocalCoordinator struct {
	logg      *slog.Logger
	bindAddr  string
	seedNodes []string
	nodeID    string
	serf      *serf.Serf
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

	c.logg.Debug("Joined cluster", "node_id", currentNodeID, "known_nodes", joined)

	worker := NewEventsWorker(currentNodeID, nil, eventsChan, c.logg, serfInstance)

	go worker.Start(nil) //TODO add context

	c.serf = serfInstance

	return nil
}

func (c *LocalCoordinator) GetNodeID() string {
	return c.nodeID
}

func (c *LocalCoordinator) Lock(key string) error {
	event := Event{
		Key:    key,
		NodeID: c.GetNodeID(),
	}

	b, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = c.serf.UserEvent(claimKeyEventName, b, false)

	if err != nil {
		return fmt.Errorf("failed to send user event: %w", err)
	}

	respCh := make(chan string)

	c.lockRespsWaiting[key] = respCh

	// I'll need to create a lock manager to be shared between coordinator and event worker
	// this lock manager will be injected on worker
}
