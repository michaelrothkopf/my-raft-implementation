package raft

// RPC provider
type RPCTransport interface {
	CallRequestVote(peerId int, args *RequestVoteArgs) (*RequestVoteReply, bool)
	CallAppendEntries(peerId int, args *AppendEntriesArgs) (*AppendEntriesReply, bool)
	CallRequestPreVote(peerId int, args *RequestPreVoteArgs) (*RequestPreVoteReply, bool)
	CallInstallSnapshot(peerId int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool)
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

type ApplyMessageType int
const (
	ApplyMessageCommand ApplyMessageType = iota
	ApplyMessageSnapshot
)

type ApplyMessage struct {
	Type			ApplyMessageType
	Data			[]byte // command data for ApplyMsgCommand, snapshot data for ApplyMsgSnapshot
	Index			int // same for both modes
	Term			int // only used in Snapshot, term of the snapshot
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

type RequestPreVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestPreVoteReply struct {
	Term			int
	VoteGranted		bool
}

type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term				int
}
