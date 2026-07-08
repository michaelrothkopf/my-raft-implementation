package sim

import (
	"sync"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"
)

// MemoryPersister implements raft.PersistenceProvider
// Provides a store for simple memory holding of the persistent fields
type MemoryPersister struct {
	mu		sync.Mutex
	state	raft.PersistentState
}

// NewMemoryPersister constructs a MemoryPersister
func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{state: raft.PersistentState{CurrentTerm:0,VotedFor:-1,Log:nil}}
}

// Save saves new persistent state to the memory persister
func (mp *MemoryPersister) Save(state raft.PersistentState) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Deep copy the log and its entries
	logCopy := make([]raft.LogEntry, len(state.Log))
	for i, logEntry := range state.Log {
		logCopy[i] = raft.LogEntry{
			Term: logEntry.Term,
			Index: logEntry.Index,
			Command: append([]byte(nil), logEntry.Command...),
		}
	}
	mp.state = raft.PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor: state.VotedFor,
		Log: logCopy,
	}

	return nil
}

// Load loads the persistent state from the memory persister
func (mp *MemoryPersister) Load() (raft.PersistentState, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Deep copy the log and its entries
	logCopy := make([]raft.LogEntry, len(mp.state.Log))
	for i, logEntry := range mp.state.Log {
		logCopy[i] = raft.LogEntry{
			Term: logEntry.Term,
			Index: logEntry.Index,
			Command: append([]byte(nil), logEntry.Command...),
		}
	}
	state := raft.PersistentState{
		CurrentTerm: mp.state.CurrentTerm,
		VotedFor: mp.state.VotedFor,
		Log: logCopy,
	}

	return state, nil
}