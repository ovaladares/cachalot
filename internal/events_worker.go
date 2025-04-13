package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	lockManager     LockManager
	electionManager *ElectionManager
	nodeName        string
	eventsChan      chan serf.Event
	logg            *slog.Logger
	serf            *serf.Serf
	mu              sync.RWMutex

	stopChan chan struct{}
	timeFn   func() time.Time
}

func NewEventsWorker(lockManager LockManager, electionManager *ElectionManager, nodeName string, eventsChan chan serf.Event, logg *slog.Logger, serf *serf.Serf) *EventsWorker {
	return &EventsWorker{
		lockManager:     lockManager,
		electionManager: electionManager,
		nodeName:        nodeName,
		eventsChan:      eventsChan,
		logg:            logg,
		serf:            serf,

		stopChan: make(chan struct{}),
		timeFn:   time.Now,
		mu:       sync.RWMutex{},
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

					w.logg.Info("Member joined", "member", member.Name)
				}
			case serf.EventMemberLeave:
				for _, member := range e.(serf.MemberEvent).Members {
					if member.Name == w.nodeName {
						w.logg.Debug("Ignoring self leave event")
						continue
					}

					w.logg.Info("Member left", "member", member.Name)
				}
			case serf.EventMemberFailed:
				for _, member := range e.(serf.MemberEvent).Members {
					if member.Name == w.nodeName {
						w.logg.Debug("Ignoring self failed event")
						continue
					}

					w.logg.Info("Member failed", "member", member.Name)
				}
			case serf.EventUser:
				name := e.(serf.UserEvent).Name
				payload := e.(serf.UserEvent).Payload

				w.logg.Debug("User event received", "name", name, "payload", string(payload))

				if name == claimKeyEventName {
					var event Event

					err := json.Unmarshal(payload, &event)

					if err != nil {
						w.logg.Error("Failed to unmarshal event", "error", err)
						continue
					}

					w.electionManager.ClaimKey(&event)
				} else if name == voteForKeyEventName {
					var event Event

					err := json.Unmarshal(payload, &event)

					if err != nil {
						w.logg.Error("Failed to unmarshal event", "error", err)
						continue
					}

					err = w.electionManager.HandleKeyVote(&event)
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
	// if lock.NodeID != w.nodeName {
	// 	w.logg.Info("Ignoring acquire lock event", "node_id", lock.NodeID, "current_node_id", w.nodeName)

	// 	w.lockManager.deletePendingLock(lock.Key)

	// 	return nil
	// }

	isLocked := w.lockManager.IsLocked(lock.Key)
	if isLocked {
		w.logg.Error("Lock already acquired", "msg", "Lock already acquired by another log, unable to acquire lock after majority vote", "key", lock.Key, "node-id", w.nodeName)
		return fmt.Errorf("lock already acquired by another node")
	}

	ok := w.lockManager.SetLock(lock.Key, lock.NodeID)

	if !ok {
		return fmt.Errorf("failed to acquire lock")
	}

	w.logg.Info("Lock acquired", "key", lock.Key, "node-id", lock.NodeID)

	return nil
}
