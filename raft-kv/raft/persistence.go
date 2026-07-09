package raft

// PersistenceProvider is anything that can save and load relevant state
type PersistenceProvider interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
	// Methods to save snapshots separately from variable state (outside of Raft specification)
	SaveSnapshot(data []byte) error
	LoadSnapshot() ([]byte, error)
}

// PersistentState is the persistent state specified by the Raft paper
type PersistentState struct {
	CurrentTerm		int
	VotedFor		int
	Log				[]LogEntry
}
