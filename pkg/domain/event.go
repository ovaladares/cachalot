package domain

type Event struct {
	Key    string `json:"key"`
	NodeID string `json:"node-id"`
	Round  int    `json:"round"`
}

type ClaimKeyEvent struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	Round      int    `json:"round"`
	TimeMillis int64  `json:"time-millis"`
}

type VoteForKeyEvent struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	Round      int    `json:"round"`
	TimeMillis int64  `json:"time-millis"`
}

type AcquireLockEvent struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	TimeMillis int64  `json:"time-millis"`
}

type RenewLockEvent struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	TimeMillis int64  `json:"time-millis"`
}

type ReleaseLockEvent struct {
	Key    string `json:"key"`
	NodeID string `json:"node-id"`
}

type LockSnapshot struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	TimeMillis int64  `json:"time-millis"`
}

type LocksSnapShotEvent struct {
	NodeID string                  `json:"node-id"`
	Locks  map[string]LockSnapshot `json:"locks"`
}

type LocksRequestEvent struct {
	NodeID string `json:"node-id"`
}

const ClaimKeyEventName = "claim-key"
const VoteForKeyEventName = "vote-for-key"
const LockAcquiredEventName = "lock-acquired"
const RenewLockEventName = "renew-lock"
const ReleaseLockEventName = "release-lock"
const LocksRequestEventName = "locks-request"
const LocksSnapShotEventName = "locks-snapshot"
