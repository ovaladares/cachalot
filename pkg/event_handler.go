package cachalot

import (
	"encoding/json"
	"log/slog"

	"github.com/otaviovaladares/cachalot/pkg/discovery"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/storage"
)

type ServiceDiscoveryEventHandler struct {
	lockManager     storage.LockManager
	electionManager ElectionManager
	nodeName        string
	logg            *slog.Logger
}

func NewServiceDiscoveryEventHandler(
	lockManager storage.LockManager,
	electionManager ElectionManager,
	nodeName string,
	logg *slog.Logger,
) *ServiceDiscoveryEventHandler {
	return &ServiceDiscoveryEventHandler{
		lockManager:     lockManager,
		electionManager: electionManager,
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
	default:
		h.logg.Warn("unknown event type", "type", event.Type)
	}
}

func (h *ServiceDiscoveryEventHandler) handleClaimEvent(cEvent *discovery.ClusterEvent) {
	var event domain.Event

	err := json.Unmarshal(cEvent.Body, &event)

	if err != nil {
		h.logg.Error("Failed to unmarshal event", "error", err)

		return
	}

	h.electionManager.StartElection(&event)
}

func (h *ServiceDiscoveryEventHandler) handleVoteForKeyEvent(cEvent *discovery.ClusterEvent) {
	var event domain.Event

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
	var lock storage.Lock

	err := json.Unmarshal(cEvent.Body, &lock)
	if err != nil {
		h.logg.Error("Failed to decode lock event")

		return
	}

	isLocked := h.lockManager.IsLocked(lock.Key)
	if isLocked {
		h.logg.Error("Lock already acquired", "msg", "Lock already acquired by another log, unable to acquire lock after majority vote", "key", lock.Key, "node-id", h.nodeName)

		return
	}

	ok := h.lockManager.SetLock(lock.Key, lock.NodeID)

	h.lockManager.DeletePendingLock(lock.Key)

	if !ok {
		return
	}

	h.logg.Info("Lock acquired", "key", lock.Key, "node-id", lock.NodeID)
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
