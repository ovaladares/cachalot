package cachalot_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/otaviovaladares/cachalot"
)

type MockCoordinator struct {
	NodeID string
}

func (m *MockCoordinator) Connect() error {
	return nil
}

func (m *MockCoordinator) GetNodeID() string {
	return m.NodeID
}

func (m *MockCoordinator) Lock(key string, duration time.Duration) (chan string, error) {
	return nil, nil
}

func (m *MockCoordinator) Renew(key string, duration time.Duration) error {
	return nil
}

func (m *MockCoordinator) Release(key string) error {
	return nil
}

func (m *MockCoordinator) GetLocks() (map[string]string, error) {
	return nil, nil
}

func TestTaskManager_AllFuncsCorrectlyWork(t *testing.T) {
	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockCoordinator1 := &MockCoordinator{
		NodeID: "test-node1",
	}

	mockCoordinator2 := &MockCoordinator{
		NodeID: "test-node2",
	}

	taskManager1 := cachalot.NewTaskManager(mockCoordinator1, logg)
	taskManager2 := cachalot.NewTaskManager(mockCoordinator2, logg)
}
