package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
)

const claimKeyEventName = "claim-key"
const voteForKeyEventName = "vote-for-key"
const lockAcquiredEventName = "lock-acquired"

type Event struct {
	Key    string `json:"key"`
	NodeID string `json:"node-id"`
	Round  int    `json:"round"`
}

type LockProposal struct {
	NodeID    string
	Timestamp int64
}

type ElectionState struct {
	Votes int
	Round int
}

type EventsWorker struct {
	lockManager    LockManager
	nodeName       string
	eventsChan     chan serf.Event
	logg           *slog.Logger
	proposalsByKey map[string]map[string]*LockProposal
	electionRounds map[string]int
	electionsState map[string]*ElectionState
	serf           *serf.Serf
	mu             sync.RWMutex

	stopChan chan struct{}
	timeFn   func() time.Time
}

func NewEventsWorker(lockManager LockManager, nodeName string, eventsChan chan serf.Event, logg *slog.Logger, serf *serf.Serf) *EventsWorker {
	return &EventsWorker{
		lockManager: lockManager,
		nodeName:    nodeName,
		eventsChan:  eventsChan,
		logg:        logg,
		serf:        serf,

		stopChan:       make(chan struct{}),
		timeFn:         time.Now,
		proposalsByKey: make(map[string]map[string]*LockProposal),
		electionRounds: make(map[string]int),
		electionsState: make(map[string]*ElectionState),
		mu:             sync.RWMutex{},
	}
}

func (w *EventsWorker) Start(ctx context.Context) {
	for {
		select {
		case e := <-w.eventsChan:
			switch e.EventType() {
			case serf.EventMemberJoin:
				for _, member := range e.(serf.MemberEvent).Members {
					if member.Name == w.nodeName {
						w.logg.Debug("Ignoring self join event")
						continue
					}

					w.logg.Debug("Member joined", "member", member.Name)
				}
			case serf.EventMemberLeave:
				for _, member := range e.(serf.MemberEvent).Members {
					if member.Name == w.nodeName {
						w.logg.Debug("Ignoring self leave event")
						continue
					}

					w.logg.Debug("Member left", "member", member.Name)
				}
			case serf.EventMemberFailed:
				for _, member := range e.(serf.MemberEvent).Members {
					if member.Name == w.nodeName {
						w.logg.Debug("Ignoring self failed event")
						continue
					}

					w.logg.Debug("Member failed", "member", member.Name)
				}
			case serf.EventUser:
				name := e.(serf.UserEvent).Name
				payload := e.(serf.UserEvent).Payload

				if name == claimKeyEventName {
					var event Event

					err := json.Unmarshal(payload, &event)

					if err != nil {
						w.logg.Error("Failed to unmarshal event", "error", err)
						continue
					}

					w.claimKey(&event)
				} else if name == voteForKeyEventName {
					var event Event

					err := json.Unmarshal(payload, &event)

					if err != nil {
						w.logg.Error("Failed to unmarshal event", "error", err)
						continue
					}

					err = w.handleKeyVote(&event)

					if err != nil {
						w.logg.Error("Failed to handle vote event", "error", err)
						continue
					}
				} else if name == lockAcquiredEventName {
					var lock Lock

					err := json.Unmarshal(payload, &lock)
					if err != nil {
						w.logg.Error("Failed to decode lock event")
						continue
					}

					err = w.handleAcquireLockEvent(&lock)

					if err != nil {
						w.logg.Error("Failed to handle lock event", "error", err)
						continue
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *EventsWorker) Stop() {
	close(w.stopChan)
}

func (w *EventsWorker) handleAcquireLockEvent(lock *Lock) error {
	if lock.NodeID != w.nodeName {
		w.logg.Debug("Ignoring acquire lock event", "node_id", lock.NodeID, "current_node_id", w.nodeName)
		return nil
	}

	isLocked := w.lockManager.isLocked(lock.Key)
	if isLocked {
		w.logg.Error("Lock already acquired", "msg", "Lock already acquired by another log, unable to acquire lock after majority vote", "key", lock.Key, "node-id", currentNodeID)
		return fmt.Errorf("lock already acquired by another node")
	}

	ok := w.lockManager.setLock(lock.Key, lock.NodeID)

	if !ok {
		return fmt.Errorf("failed to acquire lock")
	}

	w.logg.Info("Lock acquired", "key", lock.Key, "node-id", lock.NodeID)

	return nil
}

func (w *EventsWorker) claimKey(event *Event) {
	if _, ok := w.proposalsByKey[event.Key]; !ok {
		w.proposalsByKey[event.Key] = make(map[string]*LockProposal)
		w.electionRounds[event.Key] = 1
	} else {
		w.electionRounds[event.Key]++
	}

	roundNum := w.electionRounds[event.Key]
	timestamp := w.timeFn().UnixNano()
	w.proposalsByKey[event.Key][event.NodeID] = &LockProposal{
		NodeID:    event.NodeID,
		Timestamp: timestamp,
	}

	go w.election(event.Key, roundNum)
}

func (w *EventsWorker) handleKeyVote(event *Event) error {
	if event.NodeID != w.nodeName {
		w.logg.Debug("Ignoring vote for key event", "node_id", event.NodeID, "current_node_id", w.nodeName)
		return nil
	}

	electionState, ok := w.electionsState[event.Key]
	if !ok || electionState.Round != event.Round {
		electionState = &ElectionState{
			Votes: 0,
			Round: event.Round,
		}
		w.electionsState[event.Key] = electionState
	}

	electionState.Votes++

	// Check if we have a majority
	nodesCount := len(w.serf.Members())
	majority := nodesCount/2 + 1

	if electionState.Votes >= majority {
		if ch, ok := w.lockManager.pendingLock(event.Key); ok {
			w.logg.Info("Lock acquired through majority vote", "key", event.Key)

			isLocked := w.lockManager.isLocked(event.Key)
			if isLocked {
				w.logg.Error("Lock already acquired", "msg", "Lock already acquired by another log, unable to acquire lock after majority vote", "key", event.Key, "node-id", currentNodeID)
				return fmt.Errorf("lock already acquired by another node")
			}

			ch <- w.nodeName

			w.lockManager.deletePendingLock(event.Key)

			l := &Lock{
				Key:    event.Key,
				NodeID: w.nodeName,
			}

			err := w.acquireLock(l)

			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
		}
	}

	return nil
}

func (w *EventsWorker) election(key string, round int) {
	// Wait a short time to collect all proposals
	// This should be tuned based on network characteristics
	time.Sleep(100 * time.Millisecond) //TODO extract config

	if w.electionRounds[key] != round {
		return
	}

	var winningProposal *LockProposal
	var winningNodeID string

	proposals := w.proposalsByKey[key]
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

	err := w.voteForKey(&e)

	if err != nil {
		w.logg.Error("Failed to vote for key", "error", err)
		return
	}

	w.deleteProposal(key)
}

func (w *EventsWorker) deleteProposal(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.proposalsByKey[key]; ok {
		delete(w.proposalsByKey, key)
	}
}

func (w *EventsWorker) voteForKey(event *Event) error {
	b, err := json.Marshal(event)

	if err != nil {
		return err
	}

	err = w.serf.UserEvent(voteForKeyEventName, b, false)

	if err != nil {
		return err
	}

	return nil
}

func (w *EventsWorker) acquireLock(event *Lock) error {
	l := &Lock{
		Key:    event.Key,
		NodeID: w.nodeName,
	}

	ok := w.lockManager.setLock(event.Key, w.nodeName)

	if !ok {
		return fmt.Errorf("failed to acquire lock")
	}

	b, err := json.Marshal(l) //TODO change this. Im sending a internal map struct over the wire

	if err != nil {
		return fmt.Errorf("failed to marshal acquire lock event: %w", err)
	}

	err = w.serf.UserEvent(lockAcquiredEventName, b, false)

	if err != nil {
		return fmt.Errorf("failed to send lock acquired event: %w", err)
	}

	return nil
}
