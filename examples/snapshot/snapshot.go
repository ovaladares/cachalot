package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/otaviovaladares/cachalot"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <bind_address> <seed_node>")
		fmt.Println("Example: go run main.go localhost:7946 localhost:7949")
		os.Exit(1)
	}

	bindAddress := os.Args[1]
	seedNode := os.Args[2]

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	coordinator := cachalot.NewCoordinator(bindAddress, []string{seedNode}, nil)
	err := coordinator.Connect()
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)

			locks, _ := coordinator.GetLocks()

			fmt.Printf("Keys locked: %+v\n", locks)
		}
	}()

	port := strings.Split(bindAddress, ":")[1]

	keyName := fmt.Sprintf("key-%s", port)

	err = coordinator.Lock(keyName, 5*time.Minute)
	if err != nil {
		panic(fmt.Sprintf("Failed to lock key %s: %v", keyName, err))
	}

	sig := <-sigChan
	fmt.Printf("Received signal: %v. Releasing lock on key %s...\n", sig, keyName)
	err = coordinator.Release(keyName)
	if err != nil {
		panic(fmt.Sprintf("Failed to unlock key %s: %v", keyName, err))
	}
	fmt.Printf("Lock on key %s released successfully.\n", keyName)

	// Exit the program gracefully
	os.Exit(0)
}
