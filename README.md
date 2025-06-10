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

## Installation

```shell
go get github.com/otaviovaladares/cachalot
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/otaviovaladares/cachalot"
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