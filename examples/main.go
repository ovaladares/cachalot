package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/otaviovaladares/cachalot"
)

func main() {
	// Example usage of the Coordinator

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <bind_address> <seed_node>")
		fmt.Println("Example: go run main.go localhost:7946 localhost:7949")
		os.Exit(1)
	}

	bindAddress := os.Args[1]
	seedNode := os.Args[2]
	lockTime := os.Args[3]

	// Parse the lockTime (HH:MM format)
	timeParts := strings.Split(lockTime, ":")
	if len(timeParts) != 2 {
		fmt.Println("Error: Lock time must be in HH:MM format (24h)")
		os.Exit(1)
	}

	targetHour, err := strconv.Atoi(timeParts[0])
	if err != nil || targetHour < 0 || targetHour > 23 {
		fmt.Println("Error: Hour must be between 00-23")
		os.Exit(1)
	}

	targetMinute, err := strconv.Atoi(timeParts[1])
	if err != nil || targetMinute < 0 || targetMinute > 59 {
		fmt.Println("Error: Minute must be between 00-59")
		os.Exit(1)
	}

	coordinator := cachalot.NewCoordinator(bindAddress, []string{seedNode}, nil)
	err = coordinator.Connect()
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Waiting to acquire lock at %s...\n", lockTime)

	// Create a ticker to check the time every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Channel to signal when lock is acquired
	lockAcquired := make(chan bool)

	// Start goroutine to check time and acquire lock
	go func() {
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				currentHour, currentMinute := now.Hour(), now.Minute()

				// Check if it's time to acquire the lock
				if currentHour == targetHour && currentMinute == targetMinute {
					fmt.Printf("Time reached (%02d:%02d), acquiring lock...\n", currentHour, currentMinute)

					err := coordinator.Lock("example-key")
					if err != nil {
						fmt.Printf("Failed to acquire lock: %v\n", err)
					} else {
						fmt.Println("Successfully acquired lock!")
						lockAcquired <- true
						return
					}
				}
			}
		}
	}()

	go func() {
		for {
			time.Sleep(10 * time.Second)

			locks, _ := coordinator.GetLocks()

			fmt.Printf("Keys locked: %+v\n", locks)
		}
	}()

	// Wait for either lock acquisition or termination signal
	select {
	case <-lockAcquired:
		fmt.Println("Lock has been acquired. Press Ctrl+C to exit...")
		<-sigChan
	case sig := <-sigChan:
		fmt.Printf("Received signal %v, shutting down...\n", sig)
	}

	// Cleanup on exit
	fmt.Println("Gracefully shutting down")
}
