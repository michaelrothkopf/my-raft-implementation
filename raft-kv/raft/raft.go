package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Timeouts and intervals in milliseconds
const (
	HeartbeatInterval		= 50
	MinimumElectionTimeout	= 150
	MaximumElectionTimeout	= 300
)

// RaftState denotes the current role of the Raft node
type RaftState int
const (
	Follower 	RaftState = iota
	Candidate
	Leader
)

// Raft represents a Raft server node
type Raft struct {
	// Server implementation functionality providers
	mu					sync.Mutex
	me					int
	peers				[]int
	transport			RPCTransport
	
	// Persistent state
	currentTerm			int
	votedFor			int
	log					[]LogEntry

	// Volatile state
	commitIndex			int
	lastApplied			int
	state				RaftState

	// Volatile state (leaders only)
	nextIndex			[]int
	matchIndex			[]int

	// Action timers
	electionTimer		*time.Timer
	heartbeatTimer		*time.Timer

	// Votes received in the current election
	votesReceived		int
	
	// Server is down
	killed 				bool
}

// LogEntry represents a command and its position in the term and index spaces
type LogEntry struct {
	Term		int
	Index		int
	Command		[]byte
}

// NewRaft creates a new node
func NewRaft(id int, peers []int, transport RPCTransport, currentTerm int, votedFor int, log []LogEntry) *Raft {
	// Deep copy all slices
	peers_copy := make([]int, len(peers))
	copy(peers_copy, peers)
	log_copy := make([]LogEntry, len(log))
	copy(log_copy, log)

	// Create leader volatile slices
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))

	raft := &Raft{
		me: id,
		peers: peers_copy,
		transport: transport,

		currentTerm: currentTerm,
		votedFor: votedFor,
		log: log_copy,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex: nextIndex,
		matchIndex: matchIndex,

		votesReceived: 0,

		killed: false,
	}

	raft.resetElectionTimer()

	go raft.runElectionTimer()

	return raft
}

// resetElectionTimer resets the election timer to a new random value
func (rf *Raft) resetElectionTimer() {
	// Prevent mutation
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimerLocked()
}

// resetElectionTimerLocked resets the election timer to a new random value without taking mutex
func (rf *Raft) resetElectionTimerLocked() {
	if !rf.electionTimer.Stop() {
		// Drain the channel
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}

	timeoutMilliseconds := rand.Intn(MaximumElectionTimeout - MinimumElectionTimeout + 1) + MinimumElectionTimeout
	rf.electionTimer.Reset(time.Duration(timeoutMilliseconds) * time.Millisecond)
}

// runElectionTimer (goroutine) sleeps until the timeout, checks if an election should start
func (rf *Raft) runElectionTimer() {
	for {
		rf.mu.Lock()
		// If server killed, exit
		if rf.killed {
			rf.mu.Unlock()
			return
		}

		// Grab the channel
		ch := rf.electionTimer.C
		rf.mu.Unlock()

		// Block until the timer fires or a new timer is created
		<-ch
		rf.mu.Lock()

		// If killed or won, do nothing
		if rf.killed || rf.state == Leader {
			rf.mu.Unlock()
			continue
		}

		// Timer expired; start election
		rf.startElectionLocked()

		rf.mu.Unlock()
	}
}

// startElection requests votes from other nodes
// Preconditions: mutex is locked
func (rf *Raft) startElectionLocked() {
	// Mutex is already locked!

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votesReceived = 1

	// Reset election timer
	rf.resetElectionTimerLocked()

	// Send vote request to each peer
	for _, peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.sendRequestVoteAndHandleReply(peerId)
	}
}

// sendRequestVoteAndHandleReply (goroutine) calls the RPC and handles its response
func (rf *Raft) sendRequestVoteAndHandleReply(peerId int) {
	// Make sure the requirements are met
	rf.mu.Lock()
	if rf.state != Candidate || rf.killed {
		rf.mu.Unlock()
		return
	}

	// Determine the argument values
	electionTerm := rf.currentTerm
	me := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.mu.Unlock()

	// Send message
	reply, success := rf.transport.CallRequestVote(peerId, &RequestVoteArgs{
		Term: electionTerm,
		CandidateId: me,
		LastLogIndex: lastLogIndex, // last received log; not necessarily committed
		LastLogTerm: lastLogTerm,
	})

	// If not success, ignore (peer is down)
	if !success {
		return
	}

	// Acquire the lock and process the results
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ensure that election is still ongoing
	if rf.state != Candidate || rf.currentTerm != electionTerm || rf.killed {
		return
	}

	// If peer has higher term, we may not run; abandon election
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1

		// Reset the election timer; we already have mutex, so call an unlocked version
		rf.resetElectionTimerLocked()
		return
	}

	// Ignore stale reply
	if reply.Term < rf.currentTerm {
		return
	}

	// If peer has granted us vote
	if reply.VoteGranted {
		rf.votesReceived++

		// If we have won
		if rf.votesReceived > len(rf.peers) / 2 {
			rf.becomeLeaderLocked()
		}
	}
}

// handleRequestVote (RPC recipient) handles a request for a vote from another node
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Node is down
	if rf.killed {
		return nil, false
	}

	// If we are in a newer term, reject
	if args.Term < rf.currentTerm {
		return &RequestVoteReply{
			Term: rf.currentTerm,
			VoteGranted: false,
		}, true
	}

	// If we are in an older term, don't vote yet
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Grant the vote if we haven't voted this term
	// TODO: add condition for log up-to-date before voting
	voteGranted := false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		voteGranted = true
		rf.resetElectionTimerLocked()
	}

	// Return the result
	return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: voteGranted}, true
}

// handleAppendEntries (RPC recipient) handles an append entries RPC from another node
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed {
		return nil, false
	}

	// If stuck in the past
	if args.Term < rf.currentTerm {
		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
		}, true
	}

	// Otherwise, valid leader
	// If we are stuck in the past
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// TODO: add log entry

	// Reset timer
	rf.resetElectionTimerLocked()

	// Return yes
	return &AppendEntriesReply{
		Term: rf.currentTerm,
		Success: true,
	}, true
}

// becomeLeader launches goroutines to send heartbeat signals and check replies
// Precondition: mutex locked
func (rf *Raft) becomeLeaderLocked() {

	rf.state = Leader

	// TODO: volatile leader fields

	// Stop election timer (no election timer as Leader)
	if !rf.electionTimer.Stop() {
		// drain
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}

	// Broadcast timer
	for _, peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.subscribeHeartbeats(peerId)
	}
}

// subscribeHeartbeats (goroutine) sends heartbeat messages at intervals to another node as the leader
func (rf *Raft) subscribeHeartbeats(peerId int) {
	// Send a heartbeat message until no longer leader
	ticker := time.NewTicker(time.Duration(HeartbeatInterval) * time.Millisecond)

	defer ticker.Stop()

	for {
		// Ensure we are leader before sending another
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// Block until timer is up
		<-ticker.C
		rf.sendAppendEntryAndHandleResponse(peerId)
	}
}

// sendAppendEntryAndHandleResponse (goroutine) sends a heartbeat message and handles its response
func (rf *Raft) sendAppendEntryAndHandleResponse(peerId int) {
	rf.mu.Lock()
	// Ensure we are leader (stale send prevention)
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// Get the args for the heartbeat manager
	term := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()

	// Send the heartbeat (blocking)
	reply, success := rf.transport.CallAppendEntries(peerId, &AppendEntriesArgs{
		Term: term,
		LeaderId: leaderId,
	})

	// Failure means the node is down, ignore
	if !success {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we are outdated
	if reply.Term > rf.currentTerm {
		// Override and step down
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		// Reinstate the timer
		rf.resetElectionTimerLocked()
	}
	
	// TODO: implement log additions; for now, voting is set up and heartbeats reset the timers
}

// Kill kills the node
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.killed = true
	rf.mu.Unlock()
}

// Revive revives the node from the dead
func (rf *Raft) Revive() {
	rf.mu.Lock()
	rf.killed = false
	rf.mu.Unlock()
}

// GetState gets the state of the node
func (rf *Raft) GetState() RaftState {
	return rf.state
}
