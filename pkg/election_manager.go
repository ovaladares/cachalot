package cachalot

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/election"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

// ElectionManager defines the interface for managing distributed elections
type ElectionManager interface {
	ClaimKey(event *domain.Event)
	HandleKeyVote(event *domain.Event) error
	VoteForKey(event *domain.Event) error
}

// DistributedElectionManager handles the distributed election process for locks
type DistributedElectionManager struct {
	nodeName       string
	logg           *slog.Logger
	clusterManager discovery.ClusterManager
	lockManager    storage.LockManager
	stateManager   *election.StateManager
	mu             sync.RWMutex
	timeFn         func() time.Time
	conf           *ElectionConfig
}

// NewElectionManager creates a new DistributedElectionManager
func NewElectionManager(
	nodeName string,
	logg *slog.Logger,
	lockManager storage.LockManager,
	clusterManager discovery.ClusterManager,
	conf *ElectionConfig,
) ElectionManager {
	return &DistributedElectionManager{
		nodeName:       nodeName,
		logg:           logg,
		clusterManager: clusterManager,
		lockManager:    lockManager,
		stateManager:   election.NewStateManager(),
		mu:             sync.RWMutex{},
		timeFn:         time.Now,
		conf:           conf,
	}
}

// ClaimKey initiates an election for a key
func (em *DistributedElectionManager) ClaimKey(event *domain.Event) {
	// if _, ok := em.proposalsByKey[event.Key]; !ok {
	// 	em.proposalsByKey[event.Key] = make(map[string]*LockProposal)
	// 	em.electionRounds[event.Key] = 1
	// } else {
	// 	em.electionRounds[event.Key]++
	// }

	// roundNum := em.electionRounds[event.Key]
	// timestamp := em.timeFn().UnixNano()
	// em.proposalsByKey[event.Key][event.NodeID] = &LockProposal{
	// 	NodeID:    event.NodeID,
	// 	Timestamp: timestamp,
	// }

	timestamp := em.timeFn().UnixNano()
	round := em.stateManager.AddProposal(event.Key, event.NodeID, timestamp)

	go em.runElection(event.Key, round)
}

// HandleKeyVote processes a vote for a key
func (em *DistributedElectionManager) HandleKeyVote(event *domain.Event) error {
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
			em.logg.Info("Lock acquired through majority vote", "key", event.Key)

			isLocked := em.lockManager.IsLocked(event.Key)
			if isLocked {
				em.logg.Error("Lock already acquired", "msg", "Lock already acquired by another node, unable to acquire lock after majority vote", "key", event.Key, "node-id", em.nodeName)
				return fmt.Errorf("lock already acquired by another node")
			}

			ch <- em.nodeName

			em.lockManager.DeletePendingLock(event.Key)

			l := &storage.Lock{
				Key:    event.Key,
				NodeID: em.nodeName,
			}

			err := em.acquireLock(l)

			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
		}
	}

	return nil
}

// runElection runs the election process for a key
func (em *DistributedElectionManager) runElection(key string, round int) {
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

	e := domain.Event{
		Key:    key,
		NodeID: winningNodeID,
		Round:  round,
	}

	err := em.VoteForKey(&e)

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

func (em *DistributedElectionManager) VoteForKey(event *domain.Event) error {
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
func (em *DistributedElectionManager) acquireLock(lock *storage.Lock) error {
	l := &storage.Lock{
		Key:    lock.Key,
		NodeID: em.nodeName,
	}

	b, err := json.Marshal(l)

	if err != nil {
		return fmt.Errorf("failed to marshal acquire lock event: %w", err)
	}

	err = em.clusterManager.BroadcastEvent(domain.LockAcquiredEventName, b)

	if err != nil {
		return fmt.Errorf("failed to send lock acquired event: %w", err)
	}

	return nil
}
