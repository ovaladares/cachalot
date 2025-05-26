package cachalot

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

// ServiceDiscoveryEventHandler handles events from the service discovery system.
// It is a middle layer between the discovery system and the application logic.
// It exists to decouple the application from the discovery system.
// It handles events such as member join, leave, and failed events.
// It also handles events related to distributed locks, such as acquiring, renewing,
// and releasing locks.
type ServiceDiscoveryEventHandler struct {
	lockManager     storage.LockManager
	electionManager ElectionManager
	snapshotManager SnapshotManager
	nodeName        string
	logg            *slog.Logger
}

func NewServiceDiscoveryEventHandler(
	lockManager storage.LockManager,
	electionManager ElectionManager,
	snapshotManager SnapshotManager,
	nodeName string,
	logg *slog.Logger,
) *ServiceDiscoveryEventHandler {
	return &ServiceDiscoveryEventHandler{
		lockManager:     lockManager,
		electionManager: electionManager,
		snapshotManager: snapshotManager,
		nodeName:        nodeName,
		logg:            logg,
	}
}

func (h *ServiceDiscoveryEventHandler) Handle(event *discovery.ClusterEvent) {
	if event == nil {
		h.logg.Warn("event is nil")

		return
	}

	switch event.Type {
	case discovery.MemberJoinEventType:
		h.handleMemberJoin(event)
	case discovery.MemberLeaveEventType:
		h.handleMemberLeave(event)
	case discovery.MemberFailedEventType:
		h.handleMemberFailed(event)
	case domain.ClaimKeyEventName:
		h.handleClaimEvent(event)
	case domain.VoteForKeyEventName:
		h.handleVoteForKeyEvent(event)
	case domain.LockAcquiredEventName:
		h.handleAcquireLockEvent(event)
	case domain.RenewLockEventName:
		h.handleRenewLockEvent(event)
	case domain.ReleaseLockEventName:
		h.handleReleaseLockEvent(event)
	case domain.LocksRequestEventName:
		h.handleLocksRequestEvent(event)
	case domain.LocksSnapShotEventName:
		h.handleLocksSnapShotEvent(event)
	default:
		h.logg.Warn("unknown event type", "type", event.Type)
	}
}

func (h *ServiceDiscoveryEventHandler) handleLocksSnapShotEvent(event *discovery.ClusterEvent) {
	var locksSnapShotEvent domain.LocksSnapShotEvent

	if err := json.Unmarshal(event.Body, &locksSnapShotEvent); err != nil {
		h.logg.Error("failed to unmarshal locks snapshot event", "error", err)

		return
	}

	if locksSnapShotEvent.NodeID == h.nodeName {
		h.logg.Debug("ignoring self locks snapshot event")

		return
	}

	err := h.snapshotManager.AddSnapshot(&locksSnapShotEvent)
	if err != nil {
		h.logg.Error("error adding locks snapshot", "node-id", locksSnapShotEvent.NodeID)

		return
	}

	h.logg.Debug("locks snapshot received", "node-id", locksSnapShotEvent.NodeID, "locks", locksSnapShotEvent.Locks)
}

func (h *ServiceDiscoveryEventHandler) handleLocksRequestEvent(event *discovery.ClusterEvent) {
	var locksRequestEvent domain.LocksRequestEvent

	if err := json.Unmarshal(event.Body, &locksRequestEvent); err != nil {
		h.logg.Error("failed to unmarshal locks request event", "error", err)

		return
	}

	if locksRequestEvent.NodeID == h.nodeName {
		h.logg.Debug("ignoring self locks request event")

		return
	}

	err := h.lockManager.DumpLocks()
	if err != nil {
		h.logg.Error("failed to dump locks to network", "error", err)

		return
	}
}

func (h *ServiceDiscoveryEventHandler) handleClaimEvent(cEvent *discovery.ClusterEvent) {
	var event domain.ClaimKeyEvent

	err := json.Unmarshal(cEvent.Body, &event)

	if err != nil {
		h.logg.Error("Failed to unmarshal event", "error", err)

		return
	}

	h.electionManager.StartElection(&event)
}

func (h *ServiceDiscoveryEventHandler) handleVoteForKeyEvent(cEvent *discovery.ClusterEvent) {
	var event domain.VoteForKeyEvent

	err := json.Unmarshal(cEvent.Body, &event)

	if err != nil {
		h.logg.Error("Failed to unmarshal event", "error", err)
		return
	}

	err = h.electionManager.HandleVote(&event)
	if err != nil {
		h.logg.Error("Failed to handle vote event", "error", err)
		return
	}
}

func (h *ServiceDiscoveryEventHandler) handleAcquireLockEvent(cEvent *discovery.ClusterEvent) {
	var event domain.AcquireLockEvent

	err := json.Unmarshal(cEvent.Body, &event)
	if err != nil {
		h.logg.Error("Failed to decode lock event")

		return
	}

	isLocked := h.lockManager.IsLocked(event.Key)
	if isLocked {
		h.logg.Error("Lock already acquired", "msg", "Lock already acquired by another log, unable to acquire lock after majority vote", "key", event.Key, "node-id", h.nodeName)

		return
	}

	ok := h.lockManager.SetLock(event.Key, event.NodeID, time.Duration(event.TimeMillis)*time.Millisecond)

	h.lockManager.DeletePendingLock(event.Key)

	if !ok {
		return
	}

	h.logg.Info("Lock acquired", "key", event.Key, "node-id", event.NodeID)
}

func (h *ServiceDiscoveryEventHandler) handleRenewLockEvent(event *discovery.ClusterEvent) {
	var renewEvent domain.RenewLockEvent

	if err := json.Unmarshal(event.Body, &renewEvent); err != nil {
		h.logg.Error("failed to unmarshal renew lock event", "error", err)

		return
	}

	err := h.lockManager.RenewLock(renewEvent.Key, renewEvent.TimeMillis)

	if err != nil {
		h.logg.Error("failed to renew lock", "error", err)
	}
}

func (h *ServiceDiscoveryEventHandler) handleReleaseLockEvent(event *discovery.ClusterEvent) {
	var releaseEvent domain.ReleaseLockEvent

	if err := json.Unmarshal(event.Body, &releaseEvent); err != nil {
		h.logg.Error("failed to unmarshal release lock event", "error", err)

		return
	}

	err := h.lockManager.ReleaseLock(releaseEvent.Key)

	if err != nil {
		h.logg.Error("failed to release lock", "error", err)
	}
}

func (h *ServiceDiscoveryEventHandler) handleMemberJoin(event *discovery.ClusterEvent) {
	var joinEvent discovery.MemberJoinEvent

	if err := json.Unmarshal(event.Body, &joinEvent); err != nil {
		h.logg.Error("failed to unmarshal member join event", "error", err)
		return
	}

	if len(joinEvent.Nodes) == 0 {
		h.logg.Warn("member join event has no nodes")
		return
	}

	for _, node := range joinEvent.Nodes {
		if node == h.nodeName {
			h.logg.Debug("ignoring self join event")
			continue
		}

		h.logg.Info("member joined", "node", node)
	}
}

func (h *ServiceDiscoveryEventHandler) handleMemberLeave(event *discovery.ClusterEvent) {
	var leaveEvent discovery.MemberLeaveEvent

	if err := json.Unmarshal(event.Body, &leaveEvent); err != nil {
		h.logg.Error("failed to unmarshal member leave event", "error", err)

		return
	}

	if len(leaveEvent.Nodes) == 0 {
		h.logg.Warn("member leave event has no nodes")

		return
	}

	for _, node := range leaveEvent.Nodes {
		if node == h.nodeName {
			h.logg.Debug("ignoring self leave event")
			continue
		}

		h.logg.Info("member left", "node", node)
	}
}

func (h *ServiceDiscoveryEventHandler) handleMemberFailed(event *discovery.ClusterEvent) {
	var failedEvent discovery.MemberFailedEvent

	if err := json.Unmarshal(event.Body, &failedEvent); err != nil {
		h.logg.Error("failed to unmarshal member failed event", "error", err)

		return
	}

	if len(failedEvent.Nodes) == 0 {
		h.logg.Warn("member failed event has no nodes")

		return
	}

	for _, node := range failedEvent.Nodes {
		if node == h.nodeName {
			h.logg.Debug("ignoring self failed event")
			continue
		}

		h.logg.Info("member failed", "node", node)
	}
}
