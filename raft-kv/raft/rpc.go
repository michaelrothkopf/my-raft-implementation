package raft

// RPC provider
type RPCTransport interface {
	CallRequestVote(peerId int, args *RequestVoteArgs) (*RequestVoteReply, bool)
	CallAppendEntries(peerId int, args *AppendEntriesArgs) (*AppendEntriesReply, bool)
}

type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int	// 0 for voting system implementation
	LastLogTerm		int // 0 for voting system implementation
}

type RequestVoteReply struct {
	Term		int
	VoteGranted	bool
}

// LogEntry represents a command and its position in the term and index spaces
type LogEntry struct {
	Term		int
	Index		int
	Command		[]byte
}

type ApplyMsg struct {
	CommandValid	bool
	Command			[]byte
	CommandIndex	int
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool

	// Lets leader skip backward faster when there is a conflict
	ConflictIndex	int
	ConflictTerm	int
}
