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

type AppendEntriesArgs struct {
	Term 		int
	LeaderId	int
	// No other fields for voting system
}

type AppendEntriesReply struct {
	Term	int
	Success	bool
}
