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
	eventNodeID := "node1"

	event := &domain.Event{
		Key:    eventKey,
		NodeID: "node1",
	}

	eventTime := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)

	em.TimeFn = func() time.Time {
		return eventTime
	}

	em.StartElection(event)

	proposal := stateManager.GetProposals(eventKey)["node1"]

	assert.Equal(t, proposal.NodeID, eventNodeID, "should have created proposal with event node id")
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
		NodeID: "node1",
		Round:  1,
	}

	assert.Equal(t, expectedEvent, eventSent, "should have broadcasted correct event")

	assert.Nil(t, stateManager.GetProposals(eventKey), "should have deleted all proposals for the key")
}
