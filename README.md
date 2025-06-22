<p align="center">
  <img src="doc/cachalot.png" alt="Cachalot Logo" width="400"/>
</p>


# Cachalot

Cachalot is a distributed lock management system for Go applications, built on Hashicorp's Serf for peer-to-peer cluster membership and failure detection. It provides a simple API for acquiring, renewing, and releasing distributed locks across a cluster of nodes.

## Features

- **Distributed Lock Coordination**: Safely coordinate access to shared resources across a cluster
- **Peer-to-Peer Architecture**: No central server required for coordination
- **Leader Election**: Uses a consensus algorithm to determine lock ownership
- **Automatic Lock Expiration**: Locks automatically expire after a specified duration
- **Lock Renewal**: Extend the lifetime of held locks
- **Node Failure Detection**: Quick detection of failed nodes
- **Snapshot Synchronization**: New nodes can quickly sync the current lock state
- **Distributed Task Scheduling**: Schedule and coordinate tasks across the cluster

## Installation

```shell
go get github.com/ovaladares/cachalot
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/ovaladares/cachalot"
)

func main() {
    // Create a new coordinator with bind address and seed nodes
    coordinator := cachalot.NewCoordinator("localhost:7946", []string{"localhost:7946"}, nil)
    
    // Connect to the cluster
    err := coordinator.Connect()
    if err != nil {
        panic(err)
    }
    
    // Try to acquire a lock blocking
    err = coordinator.Lock("my-resource", 30*time.Second)
    if err != nil {
        fmt.Println("Failed to acquire lock:", err)
        return
    }
    
    fmt.Println("Lock acquired successfully!")
    
    // Do some work with the locked resource
    // ...
    
    // Renew the lock if needed
    err = coordinator.Renew("my-resource", 30*time.Second)
    if err != nil {
        fmt.Println("Failed to renew lock:", err)
        return
    }
    
    // Release the lock when done
    err = coordinator.Release("my-resource")
    if err != nil {
        fmt.Println("Failed to release lock:", err)
        return
    }
    
    fmt.Println("Lock released successfully!")
}
```

## Distributed Task Scheduling

Cachalot includes a powerful TaskManager that enables you to schedule tasks that will be executed across your cluster with automatic coordination to ensure each task runs on only one node.

### Task Features

* Distributed Execution: Tasks are guaranteed to run on only one node in the cluster
* Cron Scheduling: Supports standard cron expressions with optional seconds precision
* Timeout Management: Tasks automatically time out after a specified duration
* Context-aware Execution: Tasks receive a context that is canceled on timeout
* Failure Handling: Failed tasks are logged with detailed error information

Tasks coordinate across the cluster using Cachalot's distributed lock mechanism, ensuring that even if multiple nodes attempt to execute a scheduled task at the same time, only one will succeed in acquiring the lock and running the task.

### Task Manager Example

```go
    coordinator := cachalot.NewCoordinator("localhost:7946", []string{"localhost:7946"}, logger)
    err := coordinator.Connect()
    if err != nil {
        logger.Error("Failed to connect", "error", err)
        return
    }
    
    // Create a task manager
    taskManager := cachalot.NewTaskManager(coordinator, logger)
    
    // Define tasks
    tasks := []*cachalot.Task{
        {
            Name:    "daily-report",
            Timeout: 5 * time.Minute,
            Cron:    "0 0 0 * * *", // Midnight every day
            ExecFn: func(ctx context.Context, execTime time.Time, taskID string) error {
                // Implementation of your task logic
                logger.Info("Generating daily report")
                
                // Check for cancellation
                select {
                case <-ctx.Done():
                    return ctx.Err()
                default:
                    // Continue processing
                }
                
                return nil
            },
        },
        {
            Name:    "hourly-cleanup",
            Timeout: 30 * time.Second,
            Cron:    "0 0 * * * *", // Top of every hour
            ExecFn: func(ctx context.Context, execTime time.Time, taskID string) error {
                // Implementation of cleanup task
                return nil
            },
        },
    }
    
    // Register tasks with the manager
    taskManager.RegisterTasks(tasks)
    
    // Start the task manager
    taskManager.Start()
    
    // Stop the task manager when shutting down
    taskManager.Stop()
```

## Examples

The project includes example applications that demonstrate how to use Cachalot in different scenarios.

### Time-based Lock Example

Usage:

```shell
go run examples/main.go <bind_address> <seed_node> <lock_time>
```

Start three nodes (in separate terminals) to create a cluster:

```shell
# Terminal 1 - First node (seed node)
go run examples/main.go localhost:7946 localhost:7946 <TIME>

# Terminal 2 - Second node
go run examples/main.go localhost:7947 localhost:7946 <TIME>

# Terminal 3 - Third node
go run examples/main.go localhost:7948 localhost:7946 <TIME>
```

All nodes will wait until <TIME> and then attempt to acquire the same lock. Only one node will succeed, demonstrating Cachalot's distributed coordination capabilities. The application will:

1. Connect to the cluster
2. Wait until the specified time
3. Try to acquire a lock named "example-key" for 120 seconds
4. Print lock status every 10 seconds
5. Automatically attempt to renew the lock after 45 seconds

You can press Ctrl+C to exit the application.