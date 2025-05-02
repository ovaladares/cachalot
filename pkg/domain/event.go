package domain

type Event struct {
	Key    string `json:"key"`
	NodeID string `json:"node-id"`
	Round  int    `json:"round"`
}

const ClaimKeyEventName = "claim-key"
const VoteForKeyEventName = "vote-for-key"
const LockAcquiredEventName = "lock-acquired"
