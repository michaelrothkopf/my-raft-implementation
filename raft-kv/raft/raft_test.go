package raft

import "testing"

type dummyTransport struct {}

func (t* dummyTransport) CallRequestVote(peerId int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	return nil, false
}

func (t* dummyTransport) CallAppendEntries(peerId int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	return nil, false
}

func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	rf := NewRaft(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil)

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

func TestHandleRequestVote_GrantsFirstVoteInTerm(t *testing.T) {
	rf := NewRaft(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil)

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
	rf := NewRaft(0, []int{0, 1, 2}, &dummyTransport{}, 5, -1, nil)

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

func TestHandleRequestVote_RejectsNonFirstVoteInTerm(t *testing.T) {
	rf := NewRaft(0, []int{0, 1, 2}, &dummyTransport{}, 5, 3, nil)

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

func TestHandleRequestVote_GrantsNonFirstVoteInHigherTerm(t *testing.T) {
	rf := NewRaft(0, []int{0, 1, 2}, &dummyTransport{}, 5, 3, nil)

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