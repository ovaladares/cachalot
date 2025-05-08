package election

import "sync"

type LockProposal struct {
	NodeID    string
	Timestamp int64
}

type ElectionState struct {
	Votes int
	Round int
}

type StateManager struct {
	proposals    map[string]map[string]*LockProposal
	rounds       map[string]int
	votingStatus map[string]*ElectionState
	mu           sync.RWMutex
}

func NewStateManager() *StateManager {
	return &StateManager{
		proposals:    make(map[string]map[string]*LockProposal),
		rounds:       make(map[string]int),
		votingStatus: make(map[string]*ElectionState),
		mu:           sync.RWMutex{},
	}
}

func (s *StateManager) AddProposal(key, nodeID string, timestamp int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.proposals[key]; !ok {
		s.proposals[key] = make(map[string]*LockProposal)
		s.rounds[key] = 1
	} else {
		s.rounds[key]++
	}

	s.proposals[key][nodeID] = &LockProposal{
		NodeID:    nodeID,
		Timestamp: timestamp,
	}

	return s.rounds[key]
}

func (s *StateManager) RecordVote(key string, round int) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.votingStatus[key]
	if !ok || state.Round != round {
		state = &ElectionState{
			Votes: 0,
			Round: round,
		}
		s.votingStatus[key] = state
	}

	state.Votes++
	return state.Votes, true
}

func (s *StateManager) GetRound(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.rounds[key]
}

func (s *StateManager) IsValidRound(key string, round int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	currentRound, exists := s.rounds[key]

	return exists && currentRound == round
}

func (s *StateManager) GetProposals(key string) map[string]*LockProposal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.proposals[key]; !ok {
		return nil
	}

	// Create a copy to avoid concurrent modification issues
	result := make(map[string]*LockProposal)
	for nodeID, proposal := range s.proposals[key] {
		result[nodeID] = &LockProposal{
			NodeID:    proposal.NodeID,
			Timestamp: proposal.Timestamp,
		}
	}

	return result
}

func (s *StateManager) DeleteProposals(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.proposals, key)
}
