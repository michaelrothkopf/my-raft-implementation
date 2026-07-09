package raft

import (
	"testing"
	"time"
)

type dummyTransport struct {}

func (t* dummyTransport) CallRequestVote(peerId int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	return nil, false
}

func (t* dummyTransport) CallAppendEntries(peerId int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	return nil, false
}

func (t* dummyTransport) CallRequestPreVote(peerId int, args *RequestPreVoteArgs) (*RequestPreVoteReply, bool) {
	return nil, false
}

// TestGetRandomElectionTimeout tests getRandomElectionTimeout to ensure values are always between the expected value
func TestGetRandomElectionTimeout(t *testing.T) {
	timeout := getRandomElectionTimeout()
	if timeout < time.Duration(MinimumElectionTimeout) * time.Millisecond {
		t.Fatalf("timeout too low, got %d ms but should be >= %d ms", timeout, MinimumElectionTimeout)
	}
	if timeout > time.Duration(MaximumElectionTimeout) * time.Millisecond {
		t.Fatalf("timeout too low, got %d ms but should be <= %d ms", timeout, MaximumElectionTimeout)
	}
}

func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil, NewMemoryPersister())

	reply, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 3, // lower than CurrentTerm; should reject
		CandidateId: 1,
	})

	if !ok {
		t.Fatalf("expected RPC to succeed")
	}
	if reply.VoteGranted {
		t.Fatalf("should not grant vote on outdated term")
	}
	if reply.Term != 5 {
		t.Fatalf("should update term to 5, instead got %d", reply.Term)
	}
}

func TestHandleRequestVote_RejectsKilled(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, 2, nil, NewMemoryPersister())
	rf.Kill()
	_, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 3, // lower than CurrentTerm; should reject
		CandidateId: 1,
	})
	if ok {
		t.Fatalf("killed Raft should not respond to vote request")
	}
}

func TestHandleRequestVote_RejectsNonFirstVoteInTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, 3, nil, NewMemoryPersister())

	reply, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 5,
		CandidateId: 1,
	})

	if !ok {
		t.Fatalf("expected RPC to succeed")
	}
	if reply.VoteGranted {
		t.Fatalf("should not grant vote when not first in term")
	}
}

func TestHandleRequestVote_GrantsFirstVoteInTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil, NewMemoryPersister())

	reply, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 5,
		CandidateId: 1,
	})

	if !ok {
		t.Fatalf("expected RPC to succeed")
	}
	if !reply.VoteGranted {
		t.Fatalf("should grant vote when first in term")
	}
}

func TestHandleRequestVote_GrantsFirstVoteInHigherTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil, NewMemoryPersister())

	reply, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 6,
		CandidateId: 1,
	})

	if !ok {
		t.Fatalf("expected RPC to succeed")
	}
	if !reply.VoteGranted {
		t.Fatalf("should grant vote when first in a higher term")
	}
}

func TestHandleRequestVote_GrantsNonFirstVoteInHigherTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, 3, nil, NewMemoryPersister())

	reply, ok := rf.HandleRequestVote(&RequestVoteArgs{
		Term: 6,
		CandidateId: 1,
	})

	if !ok {
		t.Fatalf("expected RPC to succeed")
	}
	if !reply.VoteGranted {
		t.Fatalf("should grant vote when not first in higher term")
	}
}

func TestHandleRequestPreVote_RejectsKilled(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, 2, nil, NewMemoryPersister())
	rf.Kill()
	_, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 6,
		CandidateId: 2,
		LastLogIndex: 0,
		LastLogTerm: 0,
	})
	if ok {
		t.Fatalf("killed Raft should not respond to prevote request")
	}
}

func TestHandleRequestPreVote_RejectsOlderTerm(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 5, 2, nil, NewMemoryPersister())
	reply, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 4,
		CandidateId: 2,
		LastLogIndex: 0,
		LastLogTerm: 0,
	})
	if !ok {
		t.Fatal("Raft did not respond to prevote request")
	}
	if reply.VoteGranted {
		t.Fatalf("should not grant vote to prevote request from older term")
	}
}

func TestHandleRequestPreVote_RejectsOutOfDateLogByTerm(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 3,
		CandidateId: 2,
		LastLogIndex: 3,
		LastLogTerm: 1,
	})
	if !ok {
		t.Fatal("Raft did not respond to prevote request")
	}
	if reply.VoteGranted {
		t.Fatalf("should not grant vote to prevote request with outdated log by term")
	}
}

func TestHandleRequestPreVote_RejectsOutOfDateLogByIndex(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 3,
		CandidateId: 2,
		LastLogIndex: 2,
		LastLogTerm: 2,
	})
	if !ok {
		t.Fatal("Raft did not respond to prevote request")
	}
	if reply.VoteGranted {
		t.Fatalf("should not grant vote to prevote request with outdated log by index")
	}
}

func TestHandleRequestPreVote_GrantsNewerTerm(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 3,
		CandidateId: 2,
		LastLogIndex: 2,
		LastLogTerm: 3,
	})
	if !ok {
		t.Fatal("Raft did not respond to prevote request")
	}
	if !reply.VoteGranted {
		t.Fatalf("should grant vote to prevote request with good log (newer term)")
	}
}

func TestHandleRequestPreVote_GrantsGoodLogLength(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleRequestPreVote(&RequestPreVoteArgs{
		Term: 3,
		CandidateId: 2,
		LastLogIndex: 3,
		LastLogTerm: 2,
	})
	if !ok {
		t.Fatal("Raft did not respond to prevote request")
	}
	if !reply.VoteGranted {
		t.Fatalf("should grant vote to prevote request with good log (same term, same log length)")
	}
}

func TestHandleAppendEntries_RejectsKilled(t *testing.T) {
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, nil, NewMemoryPersister())
	rf.Kill()
	_, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 0,
		LeaderId: 1,
		PrevLogIndex: 0,
		PrevLogTerm: 0,
		Entries: []LogEntry{},
		LeaderCommit: 0,
	})
	if ok {
		t.Fatalf("killed Raft should fail append entries")
	}
}

func TestHandleAppendEntries_FailsOutdatedTerm(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1,
		LeaderId: 1,
		PrevLogIndex: 0,
		PrevLogTerm: 0,
		Entries: []LogEntry{},
		LeaderCommit: 0,
	})
	if !ok {
		t.Fatalf("Raft did not respond to AppendEntries")
	}
	if reply.Success {
		t.Fatalf("Raft should not succeed AppendEntries from outdated leader")
	}
}

func TestHandleAppendEntries_SubmitsToNewerTerm(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	rf.state = Candidate
	reply, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3,
		LeaderId: 1,
		PrevLogIndex: 3,
		PrevLogTerm: 2,
		Entries: []LogEntry{},
		LeaderCommit: 0,
	})
	if !ok {
		t.Fatalf("Raft did not respond to AppendEntries")
	}
	if !reply.Success {
		t.Fatalf("Raft should succeed AppendEntries from newer leader")
	}
	if rf.state != Follower {
		t.Fatalf("Raft should update state to Follower when receiving newer term")
	}
	if rf.votedFor != -1 {
		t.Fatalf("Raft should clear votedFor when receiving newer term")
	}
}

func TestHandleAppendEntries_FailsWithOwnLogMissingIndex(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2,
		LeaderId: 1,
		PrevLogIndex: 5,
		PrevLogTerm: 2,
		Entries: []LogEntry{},
		LeaderCommit: 0,
	})
	if !ok {
		t.Fatalf("Raft did not respond to AppendEntries")
	}
	if reply.Success {
		t.Fatalf("Raft should not succeed AppendEntries when own log doesn't have index")
	}
	if reply.ConflictIndex != 4 {
		t.Fatalf("Raft should set ConflictIndex to the first missing index from own log")
	}
}

func TestHandleAppendEntries_FailsWithTermMismatch(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3,
		LeaderId: 1,
		PrevLogIndex: 3,
		PrevLogTerm: 3,
		Entries: []LogEntry{},
		LeaderCommit: 0,
	})
	if !ok {
		t.Fatalf("Raft did not respond to AppendEntries")
	}
	if reply.Success {
		t.Fatalf("Raft should not succeed AppendEntries when own log has index in wrong term")
	}
	if reply.ConflictIndex != 2 {
		t.Fatalf("Raft should mark ConflictIndex to the index of the first item in the conflict term")
	}
	if reply.ConflictTerm != 2 {
		t.Fatalf("Raft should set the conflicting term to the one with the conflicting entry")
	}
}

func TestHandleAppendEntries_SuceedsWithValidNewEntries(t *testing.T) {
	log := []LogEntry{
		{Term:1,Index:0,Command:[]byte("")},
		{Term:1,Index:1,Command:[]byte("")},
		{Term:2,Index:2,Command:[]byte("")},
		{Term:2,Index:3,Command:[]byte("")},
	}
	rf := NewRaftWithoutReadingFromPersistence(0, []int{0, 1, 2}, &dummyTransport{}, 2, -1, log, NewMemoryPersister())
	reply, ok := rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2,
		LeaderId: 1,
		PrevLogIndex: 3,
		PrevLogTerm: 2,
		Entries: []LogEntry{
			{Term:2,Index:4,Command:[]byte("")},
		},
		LeaderCommit: 0,
	})
	if !ok {
		t.Fatalf("Raft did not respond to AppendEntries")
	}
	if !reply.Success {
		t.Fatalf("Raft should succeed AppendEntries when given a good entry under good conditions")
	}
	if len(rf.log) != 5 {
		t.Fatalf("Raft should push new entry to log")
	}
}
