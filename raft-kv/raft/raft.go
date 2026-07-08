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
	nextIndex			map[int]int
	matchIndex			map[int]int

	// Action timers
	electionTimer		*time.Timer
	heartbeatTimer		*time.Timer

	// Votes received in the current election
	votesReceived		int
	preVotesReceived	int
	preVoteTerm			int

	// Channel to apply committed commands to the state machine
	applyCh				chan ApplyMsg
	// Cond to listen for changes to the state machine
	applyCond			*sync.Cond
	
	// Server is down
	killed 				bool
	generation			int // utility; not strictly necessary in real implementation, but for fake network, allows Raft to keep track of if it has been restarted, protecting electionTimer from race conditions
}

// NewRaft creates a new node
func NewRaft(id int, peers []int, transport RPCTransport, currentTerm int, votedFor int, log []LogEntry) *Raft {
	// Deep copy all slices
	peersCopy := make([]int, len(peers))
	copy(peersCopy, peers)
	logCopy := make([]LogEntry, len(log))
	// Ensure deep copy of bytes in log entry
	for i, logEntry := range log {
		logCopy[i] = LogEntry{
			Term: logEntry.Term,
			Index: logEntry.Index,
			Command: append([]byte(nil), logEntry.Command...),
		}
	}

	// Create leader volatile slices
	nextIndex := make(map[int]int)
	matchIndex := make(map[int]int)
	for _, id := range peersCopy {
		nextIndex[id] = 0
		matchIndex[id] = 0
	}

	// Add sentinel to beginning
	// ensures len(rf.log) - 1 is always a valid index
	if len(logCopy) == 0 {
		logCopy = append(logCopy, LogEntry{ Term: 0, Index: 0, Command: nil})
	}

	raft := &Raft{
		me: id,
		peers: peersCopy,
		transport: transport,

		currentTerm: currentTerm,
		votedFor: votedFor,
		log: logCopy,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex: nextIndex,
		matchIndex: matchIndex,

		votesReceived: 0,

		electionTimer: time.NewTimer(getRandomElectionTimeout()),

		killed: false,
	}

	raft.applyCh = make(chan ApplyMsg)
	raft.applyCond = sync.NewCond(&raft.mu)

	raft.resetElectionTimer()

	go raft.runElectionTimer()
	go raft.applyChangesToStateMachineLoop()

	return raft
}

func (rf *Raft) applyChangesToStateMachineLoop() {
	rf.mu.Lock()
	myGeneration := rf.generation
	rf.mu.Unlock()

	for {
		// Wait until we are ready to commit new messages
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			if rf.killed || rf.generation != myGeneration {
				rf.mu.Unlock()
				return
			}
			rf.applyCond.Wait()
		}

		// Collect the new entries to be sent to the channel
		var toApply []LogEntry
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			toApply = append(toApply, rf.log[rf.lastApplied])
		}
		rf.mu.Unlock()

		// Send them through the channel under unlocked mutex
		for _, entry := range toApply {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: entry.Command,
				CommandIndex: entry.Index,
			}
		}
	}
}

// getRandomElectionTimeout is a helper function that gets a random time.Duration between MinimumElectionTimeout and MaximumElectionTimeout
func getRandomElectionTimeout() time.Duration {
	timeoutMilliseconds := rand.Intn(MaximumElectionTimeout - MinimumElectionTimeout + 1) + MinimumElectionTimeout
	return time.Duration(timeoutMilliseconds) * time.Millisecond
}

// Start submits a command to be entered into the log
// Precondition: node must be leader
// Returns (index, term, isLeader); let the caller know the state of the node
func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := len(rf.log)
	entry := LogEntry{
		Term: rf.currentTerm,
		Index: index,
		Command: append([]byte(nil), command...),
	}
	rf.log = append(rf.log, entry)

	return index, rf.currentTerm, true
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

	rf.electionTimer.Reset(getRandomElectionTimeout())
}

// runElectionTimer (goroutine) sleeps until the timeout, checks if an election should start
func (rf *Raft) runElectionTimer() {
	// Snapshot our generation
	rf.mu.Lock()
	myGeneration := rf.generation
	rf.mu.Unlock()

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

		// If killed, won, or changed generation, the timer is no longer needed
		if rf.killed || rf.state == Leader || rf.generation != myGeneration {
			rf.mu.Unlock()
			return
		}

		// Timer expired; start pre-election
		rf.startPreVoteLocked()

		rf.mu.Unlock()
	}
}

// startPreVoteLocked runs a pre-election to prevent unnecessary term increases for impossible elections
func (rf *Raft) startPreVoteLocked() {
	rf.preVoteTerm = rf.currentTerm + 1
	rf.preVotesReceived = 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	for _, peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go rf.sendPreVoteAndHandleReply(peerId, rf.preVoteTerm, lastLogIndex, lastLogTerm)
	}
}

// sendPreVoteAndHandleReply (goroutine) sends the prevote message and handles whether to start a real election
func (rf *Raft) sendPreVoteAndHandleReply(peerId, preVoteTerm, lastLogIndex, lastLogTerm int) {
	rf.mu.Lock()
	if rf.killed || rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	me := rf.me
	rf.mu.Unlock()

	reply, success := rf.transport.CallRequestPreVote(peerId, &RequestPreVoteArgs{
		Term: preVoteTerm,
		CandidateId: me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	})

	if !success {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed || rf.state == Leader || rf.preVoteTerm != preVoteTerm {
		return
	}

	if reply.VoteGranted {
		rf.preVotesReceived++
		// Only run the election on the winning vote
		if rf.preVotesReceived == len(rf.peers) / 2 + 1 {
			// Run an election
			rf.startElectionLocked()
		}
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

		// Only become leader on the winning vote
		if rf.votesReceived == len(rf.peers) / 2 + 1{
			rf.becomeLeaderLocked()
		}
	}
}

// HandlePreVote (RPC recipient) handles a request for a prevote from another node
func (rf *Raft) HandleRequestPreVote(args *RequestPreVoteArgs) (*RequestPreVoteReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Run the same checks as for request vote but do not change own state no matter what
	// If we are dead, do nothing
	if rf.killed {
		return nil, false
	}

	// If we are in a newer term, reject
	if args.Term < rf.currentTerm {
		return &RequestPreVoteReply{
			Term: rf.currentTerm,
			VoteGranted: false,
		}, true
	}

	// Skip older term state update

	// Ensure candidate's log is up to date
	if ((args.LastLogIndex < len(rf.log) - 1) && args.LastLogTerm == rf.log[len(rf.log) - 1].Term) || args.LastLogTerm < rf.log[len(rf.log) - 1].Term {
		return &RequestPreVoteReply{
			Term: rf.currentTerm,
			VoteGranted: false,
		}, true
	}

	// Grant the vote
	return &RequestPreVoteReply{Term: rf.currentTerm, VoteGranted: true}, true
}

// HandleRequestVote (RPC recipient) handles a request for a vote from another node
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

	// Ensure the candidate's log is at least as up to date as ours
	if ((args.LastLogIndex < len(rf.log) - 1) && args.LastLogTerm == rf.log[len(rf.log) - 1].Term) || args.LastLogTerm < rf.log[len(rf.log) - 1].Term {
		return &RequestVoteReply{
			Term: rf.currentTerm,
			VoteGranted: false,
		}, true
	}

	// Grant the vote if we haven't voted this term
	voteGranted := false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimerLocked()
	}

	// Return the result
	return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: voteGranted}, true
}

// HandleAppendEntries (RPC recipient) handles an append entries RPC from another node
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed {
		return nil, false
	}

	// If leader is stuck in the past
	if args.Term < rf.currentTerm {
		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
			ConflictIndex: -1,
			ConflictTerm: -1,
		}, true
	}

	// Valid leader, reset timer
	rf.resetElectionTimerLocked()

	// Otherwise, valid leader
	// If we are stuck in the past
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Destroy candidacy if present, we have a valid leader
	if rf.state != Follower {
		rf.state = Follower
	}

	// Before addition, check previous to ensure compatibility
	// Log index does not exist
	if args.PrevLogIndex >= len(rf.log) {
		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
			ConflictIndex: len(rf.log),
			ConflictTerm: -1,
		}, true
	}
	// Index exists, but term does not match
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Conflict term is the term of this log's value at the index the leader thinks we match
		conflictTerm := rf.log[args.PrevLogIndex].Term
		// Must crawl back conflict index to find the first index in the conflict term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex - 1].Term == conflictTerm {
			conflictIndex--
		}

		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
			ConflictIndex: conflictIndex,
			ConflictTerm: conflictTerm,
		}, true
	}

	// Everything matches; append the logs
	logIndex := args.PrevLogIndex + 1
	for entryIndex := 0; entryIndex < len(args.Entries); entryIndex++ {
		if logIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
			break
		}

		if rf.log[logIndex].Term != args.Entries[entryIndex].Term {
			rf.log = rf.log[:logIndex]
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
			break
		}

		logIndex++
	}

	// Update the commit index
	if args.LeaderCommit > rf.commitIndex {
		last := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, last)
		rf.applyCond.Broadcast() // apply new committed commands to state machine
	}

	// Return yes
	return &AppendEntriesReply{
		Term: rf.currentTerm,
		Success: true,
		ConflictIndex: -1,
		ConflictTerm: -1,
	}, true
}

// becomeLeader launches goroutines to send heartbeat signals and check replies
// Precondition: mutex locked
func (rf *Raft) becomeLeaderLocked() {

	rf.state = Leader

	// Initialize nextIndex and matchIndex
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for _, peerId := range rf.peers {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1

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
	// Snapshot our generation
	rf.mu.Lock()
	myGeneration := rf.generation
	rf.mu.Unlock()

	// Send a heartbeat message until no longer leader
	ticker := time.NewTicker(time.Duration(HeartbeatInterval) * time.Millisecond)

	defer ticker.Stop()

	for {
		rf.mu.Lock()
		// Ensure we are leader, alive, and in the same generation before sending another
		if rf.state != Leader || rf.killed || rf.generation != myGeneration {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// Block until timer is up
		<-ticker.C
		rf.sendAppendEntryAndHandleResponse(peerId)
	}
}

// findLastIndexOfTerm finds the last absolute log index containing a value in the given term
func (rf *Raft) findLastIndexOfTerm(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i
		}
	}
	return -1
}

// advanceCommitIndexLocked advances the commit index to its latest point
// Precondition: mutex locked
func (rf *Raft) advanceCommitIndexLocked() {
	// Search from the end of the log
	for i := len(rf.log) - 1; i >= rf.commitIndex; i-- {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}

		count := 1

		for _, peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.matchIndex[peer] >= i {
				count++
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = i
			rf.applyCond.Broadcast() // pass the newly committed commands to the channel
			return
		}
	}
}

// sendAppendEntryAndHandleResponse (goroutine) sends a heartbeat message and handles its response
func (rf *Raft) sendAppendEntryAndHandleResponse(peerId int) {
	rf.mu.Lock()
	// Ensure we are leader and not dead (stale send prevention)
	if rf.state != Leader || rf.killed {
		rf.mu.Unlock()
		return
	}

	// Get the log entries that need to be sent
	logsToSend := append([]LogEntry(nil), rf.log[rf.nextIndex[peerId]:]...) // make copy
	// Deep copy byte arrays to ensure each node gets a fresh copy of each LogEntry
	for i := range logsToSend {
		logsToSend[i].Command = append([]byte(nil), logsToSend[i].Command...)
	}
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	// Get other args for the message
	commitIndex := rf.commitIndex
	term := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()

	// Send the heartbeat (blocking)
	reply, success := rf.transport.CallAppendEntries(peerId, &AppendEntriesArgs{
		Term: term,
		LeaderId: leaderId,

		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: logsToSend,
		LeaderCommit: commitIndex,
	})

	// Failure means the node is down, ignore
	if !success {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore stale replies here in case we lost leadership
	if (rf.killed || rf.state != Leader || rf.currentTerm != term) {
		return
	}

	// If we are outdated
	if reply.Term > rf.currentTerm {
		// Override and step down
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		// Reinstate the timer
		rf.resetElectionTimerLocked()
		// Return out (we are done being leader, unnecessary to process state)
		return
	}
	
	// If the follower did not succeed in accepting the data, update that follower's missing info
	if !reply.Success {
		// ConflictTerm is -1 (sentinel, missing index)
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peerId] = reply.ConflictIndex
		} else {
			// ConflictTerm is some previous term, find its index and update to that
			last := rf.findLastIndexOfTerm(reply.ConflictTerm)
			if last >= 0 {
				rf.nextIndex[peerId] = last + 1
			} else {
				rf.nextIndex[peerId] = reply.ConflictIndex
			}
		}
	} else {
		rf.matchIndex[peerId] = prevLogIndex + len(logsToSend)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1

		// Check commit index
		rf.advanceCommitIndexLocked()
	}
}

// Kill kills the node
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.killed = true
	// We will become a follower once revived; assume we are no longer leader to prevent testing errors
	rf.state = Follower
	// We have been killed; when revived, we will be part of the next generation
	rf.generation++
	// Release the state machine application loop
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

// Revive revives the node from the dead
func (rf *Raft) Revive() {
	// note: state marked persistent is intentionally preserved
	rf.mu.Lock()

	rf.killed = false
	rf.state = Follower

	// Restore volatile state to simulate being rebooted
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.resetElectionTimerLocked()

	rf.mu.Unlock()

	go rf.runElectionTimer()
	go rf.applyChangesToStateMachineLoop()
}

// GetState gets the state of the node
func (rf *Raft) GetState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// GetCurrentTerm gets the current term of the node
func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

// GetApplyChannel gets the output channel for applied commands
func (rf *Raft) GetApplyChannel() (chan ApplyMsg) {
	return rf.applyCh
}