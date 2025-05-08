package cachalot_test

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	cachalot "github.com/otaviovaladares/cachalot/pkg"
	"github.com/otaviovaladares/cachalot/pkg/domain"
	"github.com/otaviovaladares/cachalot/pkg/election"
	"github.com/otaviovaladares/cachalot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestElectionManagerVoteForKey_Success(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	event := &domain.Event{
		Key:    "test-key",
		NodeID: "node1",
	}

	err := em.VoteForKey(event)
	assert.NoError(t, err, "expected no error when voting for key")

	var broadcastedEvent domain.Event
	err = json.Unmarshal(clusterManager.BroadcastEventCalledWith[0].Data, &broadcastedEvent)
	assert.NoError(t, err, "expected no error when unmarshalling broadcasted event")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 1, "expected event to be broadcasted")
	assert.Equal(t, event, &broadcastedEvent, "expected event to be broadcasted correctly")
	assert.Equal(t, domain.VoteForKeyEventName, clusterManager.BroadcastEventCalledWith[0].EventName, "expected event type to be VoteForKeyEventName")
}

func TestElectionManagerVoteForKey_ErrorOnBroadcast(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{
		BroadcastEventErr: errors.New("broadcast error"),
	}

	stateManager := election.NewStateManager()

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	event := &domain.Event{
		Key:    "test-key",
		NodeID: "node1",
	}

	err := em.VoteForKey(event)
	assert.Error(t, err, "expected error when voting for key")
}

func TestElectionManagerStartElection_SuccessOneProposal(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	eventTime := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)

	em.TimeFn = func() time.Time {
		return eventTime
	}

	em.StartElection(event)

	proposal := stateManager.GetProposals(eventKey)[nodeName]

	assert.Equal(t, proposal.NodeID, nodeName, "should have created proposal with event node id")
	assert.Equal(t, proposal.Timestamp, eventTime.UnixNano(), "should have created proposal with correct event time")

	time.Sleep(6 * time.Second)

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 1, "have called broadcast event")

	broadcastedEvent := clusterManager.BroadcastEventCalledWith[0]

	assert.Equal(t, domain.VoteForKeyEventName, broadcastedEvent.EventName, "have called broadcast event with vote for key event")

	var eventSent *domain.Event
	err := json.Unmarshal(broadcastedEvent.Data, &eventSent)

	assert.NoError(t, err)

	expectedEvent := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
		Round:  1,
	}

	assert.Equal(t, expectedEvent, eventSent, "should have broadcasted correct event")

	assert.Nil(t, stateManager.GetProposals(eventKey), "should have deleted all proposals for the key")
}

func TestElectionManagerStartElection_ErrorNoValidRound(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	eventTime := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)

	em.TimeFn = func() time.Time {
		return eventTime
	}

	em.StartElection(event)

	proposal := stateManager.GetProposals(eventKey)["node1"]

	assert.Equal(t, proposal.NodeID, nodeName, "should have created proposal with event node id")
	assert.Equal(t, proposal.Timestamp, eventTime.UnixNano(), "should have created proposal with correct event time")

	stateManager.AddProposal(eventKey, nodeName, time.Now().UnixNano())
	stateManager.AddProposal(eventKey, nodeName, time.Now().UnixNano())

	time.Sleep(6 * time.Second)

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")

	assert.NotNil(t, stateManager.GetProposals(eventKey), "should not have deleted all proposals for the key")
}

func TestElectionManagerHandleVote_DifferentNodeID(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{
		GetMembersCountResponse: 10,
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: "node2",
	}

	resp := em.HandleVote(event)

	assert.Nil(t, resp, "should not have returned an error")
	assert.Equal(t, 0, clusterManager.GetMembersCountCallCount, "should not have called GetMembersCount once")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")
}

func TestElectionManagerHandleVote_ErrorGettingCount(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{
		GetMembersCountErr: errors.New("error getting members count"),
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	resp := em.HandleVote(event)

	assert.Error(t, resp, "should return an error")
	assert.Equal(t, 1, clusterManager.GetMembersCountCallCount, "should have called GetMembersCount once")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")
}

func TestElectionManagerHandleVote_NoMajority(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{
		GetMembersCountResponse: 10,
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	resp := em.HandleVote(event)

	assert.Nil(t, resp, "should not have returned an error")
	assert.Equal(t, 1, clusterManager.GetMembersCountCallCount, "should have called GetMembersCount once")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")
}

// func TestElectionManagerStartElection_DetermineWinnerMultipleProposals(t *testing.T) {
// 	panic("not implemented")
// }

func TestElectionManagerHandleVote_NoPendingLock(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{
		PendingLockResponseExists: false,
	}
	clusterManager := &MockClusterManager{
		GetMembersCountResponse: 1,
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	resp := em.HandleVote(event)

	assert.Nil(t, resp, "should not have returned an error")
	assert.Equal(t, 1, clusterManager.GetMembersCountCallCount, "should have called GetMembersCount once")

	assert.Len(t, lockManager.PendingLockCalledWith, 1, "should have called PendingLock")
	assert.Equal(t, lockManager.PendingLockCalledWith[0], eventKey, "should have called PendingLock with the correct key")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")
	assert.Equal(t, 0, lockManager.DeletePendingLockCallCount, "should not have called DeletePendingLock")
}

func TestElectionManagerHandleVote_PendingLockButItsLocked(t *testing.T) {
	nodeName := "node1"
	pendingLockCh := make(chan string)

	lockManager := &MockLockManager{
		PendingLockResponseExists: true,
		PendingLockResponse:       pendingLockCh,
	}

	clusterManager := &MockClusterManager{
		GetMembersCountResponse: 1,
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	lockManager.SetLock(eventKey, nodeName)

	resp := em.HandleVote(event)

	assert.Error(t, resp, "should have returned an error")
	assert.Equal(t, 1, clusterManager.GetMembersCountCallCount, "should have called GetMembersCount once")

	assert.Len(t, lockManager.PendingLockCalledWith, 1, "should have called PendingLock")
	assert.Equal(t, lockManager.PendingLockCalledWith[0], eventKey, "should have called PendingLock with the correct key")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 0, "should not have called broadcast event")
	assert.Equal(t, 0, lockManager.DeletePendingLockCallCount, "should not have called DeletePendingLock")
}

func TestElectionManagerHandleVote_SuccessMajority(t *testing.T) {
	nodeName := "node1"
	pendingLockCh := make(chan string, 1)

	lockManager := &MockLockManager{
		PendingLockResponseExists: true,
		PendingLockResponse:       pendingLockCh,
	}

	clusterManager := &MockClusterManager{
		GetMembersCountResponse: 1,
	}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	stateManager := election.NewStateManager()

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
		stateManager,
		&cachalot.ElectionConfig{
			TimeToWaitForVotes: 5 * time.Second,
		},
	)

	eventKey := "test-key"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: nodeName,
	}

	resp := em.HandleVote(event)

	receivedEvent := <-pendingLockCh

	assert.Nil(t, resp, "should not return an error")
	assert.Equal(t, 1, clusterManager.GetMembersCountCallCount, "should have called GetMembersCount once")

	assert.Len(t, lockManager.PendingLockCalledWith, 1, "should have called PendingLock")
	assert.Equal(t, lockManager.PendingLockCalledWith[0], eventKey, "should have called PendingLock with the correct key")

	assert.Equal(t, nodeName, receivedEvent, "should have sent the correct node ID to the channel")

	assert.Equal(t, 1, lockManager.DeletePendingLockCallCount, "should have called DeletePendingLock")
	assert.Equal(t, eventKey, lockManager.DeletePendingLockCalledWith[0], "should have called DeletePendingLock with right paramters")

	assert.Len(t, clusterManager.BroadcastEventCalledWith, 1, "should have called broadcast event")

	assert.Equal(t, domain.LockAcquiredEventName, clusterManager.BroadcastEventCalledWith[0].EventName)

	var broadcastedEvent storage.Lock
	err := json.Unmarshal(clusterManager.BroadcastEventCalledWith[0].Data, &broadcastedEvent)

	assert.NoError(t, err)

	assert.Equal(t, eventKey, broadcastedEvent.Key, "broadcasted lock event with right key")
	assert.Equal(t, nodeName, broadcastedEvent.NodeID, "broadcasted lock event with right node name")

}
