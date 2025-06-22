package cachalot

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ovaladares/cachalot/pkg/discovery"
	"github.com/ovaladares/cachalot/pkg/domain"
	"github.com/ovaladares/cachalot/pkg/election"
	"github.com/ovaladares/cachalot/pkg/storage"
)

type ElectionConfig struct {
	// TimeToWaitForVotes is the time to wait for votes from other nodes
	// before proceeding with the election process. This is important for
	// ensuring that all nodes have a chance to participate in the election
	TimeToWaitForVotes time.Duration
}

// ElectionManager defines the interface for managing distributed elections
type ElectionManager interface {
	// StartElection initiates an election for a key
	StartElection(event *domain.ClaimKeyEvent)
	// HandleVote processes a vote for a key
	HandleVote(event *domain.VoteForKeyEvent) error
	// VoteForKey sends a vote for a key
	VoteForKey(string, string, int64, int) error
}

// DistributedElectionManager handles the distributed election process for locks
// It implements the ElectionManager interface and uses a state manager to
// track the state of elections and a lock manager to manage locks
// It also uses a cluster manager to communicate with other nodes in the cluster
type DistributedElectionManager struct {
	// nodeName is the unique identifier for this node in the cluster
	nodeName string
	// logg is the logger used for logging messages
	logg *slog.Logger
	// clusterManager is used to manage the cluster and communicate with other nodes
	clusterManager discovery.ClusterManager
	// lockManager is used to manage locks in the cluster
	// It is responsible for acquiring, renewing, and releasing locks
	lockManager storage.LockManager
	// stateManager is used to manage the state of elections
	// It is responsible for tracking proposals, votes, and the current state of elections
	stateManager *election.StateManager
	mu           sync.RWMutex
	TimeFn       func() time.Time
	conf         *ElectionConfig
}

// NewElectionManager creates a new DistributedElectionManager
func NewElectionManager(
	nodeName string,
	logg *slog.Logger,
	lockManager storage.LockManager,
	clusterManager discovery.ClusterManager,
	stateManager *election.StateManager,
	conf *ElectionConfig,
) *DistributedElectionManager {
	return &DistributedElectionManager{
		nodeName:       nodeName,
		logg:           logg,
		clusterManager: clusterManager,
		lockManager:    lockManager,
		stateManager:   stateManager,
		mu:             sync.RWMutex{},
		TimeFn:         time.Now,
		conf:           conf,
	}
}

// StartElection initiates an election for a key
func (em *DistributedElectionManager) StartElection(event *domain.ClaimKeyEvent) {
	timestamp := em.TimeFn().UnixNano()
	round := em.stateManager.AddProposal(event.Key, event.NodeID, timestamp)

	go em.runElection(event.Key, event.TimeMillis, round)
}

// HandleVote processes a vote for a key
func (em *DistributedElectionManager) HandleVote(event *domain.VoteForKeyEvent) error {
	if event.NodeID != em.nodeName {
		em.logg.Debug("Ignoring vote for key event", "node_id", event.NodeID, "current_node_id", em.nodeName)
		return nil
	}

	votes, _ := em.stateManager.RecordVote(event.Key, event.Round)

	nodesCount, err := em.clusterManager.GetMembersCount()

	if err != nil {
		em.logg.Error("Failed to get members count", "error", err)

		return fmt.Errorf("failed to get members count: %w", err)
	}

	majority := nodesCount/2 + 1

	if votes >= majority {
		if ch, ok := em.lockManager.PendingLock(event.Key); ok {
			isLocked := em.lockManager.IsLocked(event.Key)
			if isLocked {
				em.logg.Error("Lock already acquired", "msg", "Lock already acquired by another node, unable to acquire lock after majority vote", "key", event.Key, "node-id", em.nodeName)
				return fmt.Errorf("lock already acquired by another node")
			}

			ch <- em.nodeName

			em.lockManager.DeletePendingLock(event.Key)

			l := &domain.AcquireLockEvent{
				Key:        event.Key,
				NodeID:     em.nodeName,
				TimeMillis: event.TimeMillis,
			}

			err := em.acquireLock(l)

			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}

			em.logg.Info("Lock acquired through majority vote", "key", event.Key)
		}
	}

	return nil
}

// runElection runs the election process for a key
func (em *DistributedElectionManager) runElection(key string, timeMillis int64, round int) {
	// Wait a short time to collect all proposals
	// This should be tuned based on network characteristics
	time.Sleep(em.conf.TimeToWaitForVotes)

	if !em.stateManager.IsValidRound(key, round) {
		return
	}

	proposals := em.stateManager.GetProposals(key)
	if proposals == nil {
		em.logg.Debug("No proposals found for key", "key", key)
		return
	}

	winningNodeID := em.determineWinner(proposals)

	err := em.VoteForKey(key, winningNodeID, timeMillis, round)

	if err != nil {
		em.logg.Error("Failed to vote for key", "error", err)
		return
	}

	em.stateManager.DeleteProposals(key)
}

func (em *DistributedElectionManager) determineWinner(proposals map[string]*election.LockProposal) string {
	var winningProposal *election.LockProposal
	var winningNodeID string

	for nodeID, proposal := range proposals {
		if winningProposal == nil {
			winningProposal = proposal
			winningNodeID = nodeID
			continue
		}

		currID, _ := strconv.Atoi(strings.Split(nodeID, "-")[1])
		winningID, _ := strconv.Atoi(strings.Split(winningNodeID, "-")[1])

		if currID < winningID {
			winningProposal = proposal
			winningNodeID = nodeID
		} else if currID == winningID && proposal.Timestamp < winningProposal.Timestamp {
			winningProposal = proposal
			winningNodeID = nodeID
		}
	}

	return winningNodeID
}

func (em *DistributedElectionManager) VoteForKey(key, nodeID string, timeMillis int64, round int) error {
	event := domain.VoteForKeyEvent{
		Key:        key,
		NodeID:     nodeID,
		Round:      round,
		TimeMillis: timeMillis,
	}

	b, err := json.Marshal(event)

	if err != nil {
		return err
	}

	err = em.clusterManager.BroadcastEvent(domain.VoteForKeyEventName, b)

	if err != nil {
		return err
	}

	return nil
}

// acquireLock acquires a lock after winning an election
func (em *DistributedElectionManager) acquireLock(acquireLockEvent *domain.AcquireLockEvent) error {
	b, err := json.Marshal(acquireLockEvent)

	if err != nil {
		return fmt.Errorf("failed to marshal acquire lock event: %w", err)
	}

	err = em.clusterManager.BroadcastEvent(domain.LockAcquiredEventName, b)

	if err != nil {
		return fmt.Errorf("failed to send lock acquired event: %w", err)
	}

	return nil
}
