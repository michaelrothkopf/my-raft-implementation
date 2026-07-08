package raft

// PersistenceProvider is anything that can save and load relevant state
type PersistenceProvider interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}

// PersistentState is the persistent state specified by the Raft paper
type PersistentState struct {
	CurrentTerm		int
	VotedFor		int
	Log				[]LogEntry
}
