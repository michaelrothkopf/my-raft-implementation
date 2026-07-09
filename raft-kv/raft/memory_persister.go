package raft

import (
	"sync"
)

// MemoryPersister implements PersistenceProvider
// Provides a store for simple memory holding of the persistent fields
type MemoryPersister struct {
	mu			sync.Mutex
	state		PersistentState
	snapshot	[]byte
}

// NewMemoryPersister constructs a MemoryPersister
func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{state: PersistentState{CurrentTerm:0,VotedFor:-1,Log:nil}}
}

// Save saves new persistent state to the memory persister
func (mp *MemoryPersister) Save(state PersistentState) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Deep copy the log and its entries
	logCopy := make([]LogEntry, len(state.Log))
	for i, logEntry := range state.Log {
		logCopy[i] = LogEntry{
			Term: logEntry.Term,
			Index: logEntry.Index,
			Command: append([]byte(nil), logEntry.Command...),
		}
	}
	mp.state = PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor: state.VotedFor,
		Log: logCopy,
	}

	return nil
}

// Load loads the persistent state from the memory persister
func (mp *MemoryPersister) Load() (PersistentState, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Deep copy the log and its entries
	logCopy := make([]LogEntry, len(mp.state.Log))
	for i, logEntry := range mp.state.Log {
		logCopy[i] = LogEntry{
			Term: logEntry.Term,
			Index: logEntry.Index,
			Command: append([]byte(nil), logEntry.Command...),
		}
	}
	state := PersistentState{
		CurrentTerm: mp.state.CurrentTerm,
		VotedFor: mp.state.VotedFor,
		Log: logCopy,
	}

	return state, nil
}

func (mp *MemoryPersister) SaveSnapshot(data []byte) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Data should not be empty
	if len(data) == 0 {
		panic("attempted to save snapshot using MemoryPersister.SaveSnapshot with empty data")
	}

	// Deep copy the snapshot to state
	mp.snapshot = append([]byte(nil), data...)

	return nil
}

func (mp *MemoryPersister) LoadSnapshot() ([]byte, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Return a deep copy of the data
	return append([]byte(nil), mp.snapshot...), nil
}
