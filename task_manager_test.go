package cachalot_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ovaladares/cachalot"
	"github.com/stretchr/testify/assert"
)

type MockLocker struct {
	locks     map[string]string
	lockCalls int
	Mu        sync.Mutex
}

func (m *MockLocker) Lock(key string, duration time.Duration) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.lockCalls++

	if m.locks == nil {
		m.locks = make(map[string]string)
	}

	if _, exists := m.locks[key]; exists {
		return errors.New("lock already acquired")
	}

	m.locks[key] = key

	return nil
}

func (m *MockLocker) Release(key string) error {
	return nil
}
func TestTaskManager_AllFuncsCorrectlyWork(t *testing.T) {
	logg := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockCoordinator := &MockLocker{}

	taskManager1 := cachalot.NewTaskManager(mockCoordinator, logg)
	taskManager2 := cachalot.NewTaskManager(mockCoordinator, logg)

	called := 0

	var mu sync.Mutex

	task := &cachalot.Task{
		Name: "test_task",
		ExecFn: func(ctx context.Context, execTime time.Time, taskID string) error {
			mu.Lock()
			called++
			mu.Unlock()

			return nil
		},
		Timeout: 5 * time.Second,
		Cron:    "* * * * * *",
	}

	err := taskManager1.RegisterTasks([]*cachalot.Task{task})

	assert.NoError(t, err, "Task should be registered without error")

	err = taskManager2.RegisterTasks([]*cachalot.Task{task})

	assert.NoError(t, err, "Task should be registered without error in the second manager")

	taskManager1.Start()
	taskManager2.Start()

	time.Sleep(2 * time.Second)

	mu.Lock()
	assert.Equal(t, 1, called, "Task should be executed only once across the cluster")
	mu.Unlock()

	mockCoordinator.Mu.Lock()
	defer mockCoordinator.Mu.Unlock()

	assert.GreaterOrEqual(t, mockCoordinator.lockCalls, 2, "Lock should be called at least two times by the task execution")
}
