package domain

type Event struct {
	Key    string `json:"key"`
	NodeID string `json:"node-id"`
	Round  int    `json:"round"`
}

type RenewLockEvent struct {
	Key        string `json:"key"`
	NodeID     string `json:"node-id"`
	TimeMillis int64  `json:"time-millis"`
}

const ClaimKeyEventName = "claim-key"
const VoteForKeyEventName = "vote-for-key"
const LockAcquiredEventName = "lock-acquired"
const RenewLockEventName = "renew-lock"
