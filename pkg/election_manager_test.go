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
	"github.com/stretchr/testify/assert"
)

func TestElectionManagerVoteForKey_Success(t *testing.T) {
	nodeName := "node1"
	lockManager := &MockLockManager{}
	clusterManager := &MockClusterManager{}

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
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

	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	em := cachalot.NewElectionManager(
		nodeName,
		logg,
		lockManager,
		clusterManager,
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
