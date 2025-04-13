package internal

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
)

// ElectionManager handles the distributed election process for locks
type ElectionManager struct {
	nodeName       string
	logg           *slog.Logger
	proposalsByKey map[string]map[string]*LockProposal
	electionRounds map[string]int
	electionsState map[string]*ElectionState
	serf           *serf.Serf
	lockManager    LockManager
	mu             sync.RWMutex
	timeFn         func() time.Time
}

// NewElectionManager creates a new ElectionManager
func NewElectionManager(nodeName string, logg *slog.Logger, serf *serf.Serf, lockManager LockManager) *ElectionManager {
	return &ElectionManager{
		nodeName:       nodeName,
		logg:           logg,
		serf:           serf,
		lockManager:    lockManager,
		proposalsByKey: make(map[string]map[string]*LockProposal),
		electionRounds: make(map[string]int),
		electionsState: make(map[string]*ElectionState),
		mu:             sync.RWMutex{},
		timeFn:         time.Now,
	}
}

// ClaimKey initiates an election for a key
func (em *ElectionManager) ClaimKey(event *Event) {
	if _, ok := em.proposalsByKey[event.Key]; !ok {
		em.proposalsByKey[event.Key] = make(map[string]*LockProposal)
		em.electionRounds[event.Key] = 1
	} else {
		em.electionRounds[event.Key]++
	}

	roundNum := em.electionRounds[event.Key]
	timestamp := em.timeFn().UnixNano()
	em.proposalsByKey[event.Key][event.NodeID] = &LockProposal{
		NodeID:    event.NodeID,
		Timestamp: timestamp,
	}

	go em.runElection(event.Key, roundNum)
}

// HandleKeyVote processes a vote for a key
func (em *ElectionManager) HandleKeyVote(event *Event) error {
	if event.NodeID != em.nodeName {
		em.logg.Debug("Ignoring vote for key event", "node_id", event.NodeID, "current_node_id", em.nodeName)
		return nil
	}

	electionState, ok := em.electionsState[event.Key]
	if !ok || electionState.Round != event.Round {
		electionState = &ElectionState{
			Votes: 0,
			Round: event.Round,
		}
		em.electionsState[event.Key] = electionState
	}

	electionState.Votes++

	nodesCount := len(em.serf.Members())
	majority := nodesCount/2 + 1

	if electionState.Votes >= majority {
		if ch, ok := em.lockManager.PendingLock(event.Key); ok {
			em.logg.Info("Lock acquired through majority vote", "key", event.Key)

			isLocked := em.lockManager.IsLocked(event.Key)
			if isLocked {
				em.logg.Error("Lock already acquired", "msg", "Lock already acquired by another node, unable to acquire lock after majority vote", "key", event.Key, "node-id", em.nodeName)
				return fmt.Errorf("lock already acquired by another node")
			}

			ch <- em.nodeName

			em.lockManager.DeletePendingLock(event.Key)

			l := &Lock{
				Key:    event.Key,
				NodeID: em.nodeName,
			}

			err := em.AcquireLock(l)

			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
		}
	}

	return nil
}

// runElection runs the election process for a key
func (em *ElectionManager) runElection(key string, round int) {
	// Wait a short time to collect all proposals
	// This should be tuned based on network characteristics
	time.Sleep(2 * time.Second) //TODO extract config

	if em.electionRounds[key] != round {
		return
	}

	var winningProposal *LockProposal
	var winningNodeID string

	proposals := em.proposalsByKey[key]
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

	e := Event{
		Key:    key,
		NodeID: winningNodeID,
		Round:  round,
	}

	err := em.VoteForKey(&e)

	if err != nil {
		em.logg.Error("Failed to vote for key", "error", err)
		return
	}

	em.DeleteProposal(key)
}

// VoteForKey broadcasts a vote for a key
func (em *ElectionManager) VoteForKey(event *Event) error {
	b, err := json.Marshal(event)

	if err != nil {
		return err
	}

	err = em.serf.UserEvent(voteForKeyEventName, b, false)

	if err != nil {
		return err
	}

	return nil
}

// AcquireLock acquires a lock after winning an election
func (em *ElectionManager) AcquireLock(lock *Lock) error {
	l := &Lock{
		Key:    lock.Key,
		NodeID: em.nodeName,
	}

	// ok := em.lockManager.setLock(lock.Key, em.nodeName)

	// if !ok {
	// 	return fmt.Errorf("failed to acquire lock")
	// }

	b, err := json.Marshal(l)

	if err != nil {
		return fmt.Errorf("failed to marshal acquire lock event: %w", err)
	}

	err = em.serf.UserEvent(lockAcquiredEventName, b, false)

	if err != nil {
		return fmt.Errorf("failed to send lock acquired event: %w", err)
	}

	return nil
}

// DeleteProposal removes a proposal for a key
func (em *ElectionManager) DeleteProposal(key string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	delete(em.proposalsByKey, key)
}
