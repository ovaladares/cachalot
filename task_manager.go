package cachalot

import (
	"context"
	"log/slog"
	"time"

	"github.com/robfig/cron"
)

// Locker defines the interface for a distributed lock manager.
// Cachalot Coordinator implements this interface to provide locking capabilities
type Locker interface {
	// Lock acquires a lock for the given key with a specified duration.
	Lock(key string, duration time.Duration) error
	// Release releases the lock for the given key.
	Release(key string) error
}

// ExecTaskFunc defines the function signature for executing a task.
// TaskManager will call this function to execute tasks at the scheduled time.
type ExecTaskFunc func(ctx context.Context, execTime time.Time, taskID string) error

// Task represents a scheduled task in the TaskManager.
type Task struct {
	// Name is the unique identifier for the task.
	// Warning: Do not use the same name for different tasks.
	// It is used to acquire a lock in the Coordinator.
	Name string
	// Function to execute the task.
	ExecFn ExecTaskFunc
	// Timeout defines the maximum duration for the task execution.
	// If the task does not complete within this time, it will be considered failed.
	Timeout time.Duration
	// Cron with special support for seconds if needed.
	// Ex: "* * * * * *" for every second
	// Normal format also supported.
	Cron string
}

// TaskManager is responsible to manage task executions in a distributed environment.
// It can coordinate the execution of tasks across all nodes joined in the cluster.
type TaskManager struct {
	coordinator Locker
	tasks       []*Task
	cronManager *cron.Cron
	logg        *slog.Logger
}

// NewTaskManager creates a new TaskManager instance.
func NewTaskManager(coordinator Locker, logg *slog.Logger) *TaskManager {
	return &TaskManager{
		coordinator: coordinator,
		cronManager: cron.New(), // Initialize cron with seconds support
		tasks:       make([]*Task, 0),
		logg:        logg.With("component", "cachalot_task_manager"),
	}
}

// RegisterTask registers a new task to be managed by the TaskManager.
func (tm *TaskManager) RegisterTasks(tasks []*Task) error {
	tm.tasks = append(tm.tasks, tasks...)
	for _, task := range tasks {
		err := tm.cronManager.AddFunc(task.Cron, func() {
			tm.handleTask(task)
		})

		if err != nil {
			tm.logg.Error("Failed to register task", "task", task.Name, "error", err)
			return err
		}
	}

	tm.logg.Debug("Tasks registered", "count", len(tasks))

	return nil
}

// Start initializes and starts the TaskManager.
func (tm *TaskManager) Start() {
	tm.cronManager.Start()
	tm.logg.Info("TaskManager started")
}

// Stop stops the TaskManager and all its tasks.
func (tm *TaskManager) Stop() {
	tm.cronManager.Stop()
	tm.logg.Info("TaskManager stopped")
}

func (tm *TaskManager) handleTask(task *Task) {
	ctx, cancel := context.WithTimeout(context.Background(), task.Timeout)
	defer cancel()

	go func() {
		err := tm.coordinator.Lock(task.Name, task.Timeout)
		if err != nil {
			tm.logg.Error("Failed to acquire lock for task", "task", task.Name)
			return
		}

		tm.logg.Info("Lock acquired for task", "task", task.Name)
		defer tm.coordinator.Release(task.Name)

		done := make(chan error, 1)

		go func() {
			tm.logg.Info("Executing task", "task", task.Name)
			done <- task.ExecFn(ctx, time.Now(), task.Name)
		}()

		select {
		case err := <-done:
			if err != nil {
				tm.logg.Error("Failed to execute task", "task", task.Name, "error", err)
				return
			}
			tm.logg.Info("Task executed successfully", "task", task.Name)
		case <-ctx.Done():
			tm.logg.Error("Task execution timed out", "task", task.Name, "timeout", task.Timeout)
			return
		}
	}()
}
