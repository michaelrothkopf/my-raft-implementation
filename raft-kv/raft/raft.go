package raft

import (
	"math/rand"
	"strconv"
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
	persister			PersistenceProvider
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
	applyCh				chan ApplyMessage
	// Cond to listen for changes to the state machine
	applyCond			*sync.Cond
	
	// Server is down
	killed 				bool
	generation			int // utility; not strictly necessary in real implementation, but for fake network, allows Raft to keep track of if it has been restarted, protecting electionTimer from race conditions
}

// NewRaftWithoutReadingFromPersistence creates a new node
func NewRaftWithoutReadingFromPersistence(id int, peers []int, transport RPCTransport, currentTerm int, votedFor int, log []LogEntry, persister PersistenceProvider) *Raft {
	// Ensure non-nil transport and persister
	if transport == nil {
		panic("transport may not be nil when initializing a new Raft")
	}
	if persister == nil {
		panic("persister may not be nil when initializing a new Raft")
	}
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
		logCopy = append(logCopy, LogEntry{ Term: 0, Index: 0, Command: nil })
	}

	raft := &Raft{
		me: id,
		peers: peersCopy,
		transport: transport,

		persister: persister,
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

	raft.applyCh = make(chan ApplyMessage)
	raft.applyCond = sync.NewCond(&raft.mu)

	raft.resetElectionTimer()

	go raft.runElectionTimer()
	go raft.applyChangesToStateMachineLoop()

	return raft
}

// NewRaft constructs a new Raft
func NewRaft(id int, peers []int, transport RPCTransport, persister PersistenceProvider) *Raft {
	// Load state from the persister
	state, err := persister.Load()
	if err != nil {
		panic("failed to load persistent state with error " + err.Error())
	}

	// Call the parameterized constructor
	return NewRaftWithoutReadingFromPersistence(id, peers, transport, state.CurrentTerm, state.VotedFor, state.Log, persister)
}

// logIndexToMemoryIndexLocked converts an index from a log index (stored in LogEntry.Index) to the index in memory (rf.log)
// Precondition: mutex locked
func (rf *Raft) logIndexToMemoryIndexLocked(logIndex int) int {
	return logIndex - rf.log[0].Index
}

// logIndexToMemoryIndex converts an index from a log index (stored in LogEntry.Index) to the index in memory (rf.log)
func (rf *Raft) logIndexToMemoryIndex(logIndex int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logIndexToMemoryIndexLocked(logIndex)
}

// lastLogIndexLocked returns the log index of the last entry in the log
// Precondition: mutex locked
func (rf *Raft) lastLogIndexLocked() int {
	return rf.log[len(rf.log) - 1].Index
}

// lastLogIndex returns the log index of the last entry in the log
func (rf *Raft) lastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLogIndexLocked()
}

// becomeFollowerLocked safely becomes a follower (checks if we were leader before; if so, we must restart the election timer)
func (rf *Raft) becomeFollowerLocked() {
	oldState := rf.state
	rf.state = Follower
	// If we are leader, must restart the election timer
	if oldState == Leader {
		go rf.runElectionTimer()
	}
}

// persistLocked saves the persistent state variables to the persister
func (rf *Raft) persistLocked() {
	rf.persister.Save(PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor: rf.votedFor,
		Log: rf.log,
	})
}

// persistSnapshotLocked saves snapshot data to the persister
func (rf *Raft) persistSnapshotLocked(snapshotData []byte) {
	rf.persister.SaveSnapshot(snapshotData)
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
			toApply = append(toApply, rf.log[rf.logIndexToMemoryIndexLocked(rf.lastApplied)])
		}
		rf.mu.Unlock()

		// Send them through the channel under unlocked mutex
		for _, entry := range toApply {
			rf.applyCh <- ApplyMessage{
				Type: ApplyMessageCommand,
				Data: entry.Command,
				Index: entry.Index,
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

	index := rf.lastLogIndexLocked() + 1
	entry := LogEntry{
		Term: rf.currentTerm,
		Index: index,
		Command: append([]byte(nil), command...),
	}
	rf.log = append(rf.log, entry)
	rf.persistLocked()

	// Edge case: we have one node
	// If we have one node, we can commit immediately
	if len(rf.peers) == 1 {
		rf.advanceCommitIndexLocked()
	}

	return index, rf.currentTerm, true
}

// Snapshot logs a snapshot in rf.log after it has been created; it deletes the unnecessary LogEntry objects
func (rf *Raft) Snapshot(index int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the snapshot compacts nothing, do nothing
	if index <= rf.log[0].Index {
		return
	}

	// Can't snapshot future commands
	if index > rf.lastLogIndexLocked() {
		panic("Raft.Snapshot called with out-of-bounds log index (Snapshot index: " + strconv.Itoa(index) + ", last log index: " + strconv.Itoa(rf.lastLogIndexLocked()) + ")")
	}

	// Discard the log
	sliceStart := rf.logIndexToMemoryIndexLocked(index)
	newSentinel := LogEntry{ Term: rf.log[sliceStart].Term, Index: index }
	newLog := make([]LogEntry, 0, len(rf.log) - sliceStart) // want empty slice (will be appended) but with enough capacity to be appended
	newLog = append(newLog, newSentinel)
	newLog = append(newLog, rf.log[sliceStart+1:]...)
	rf.log = newLog
	rf.persistLocked()

	// Save the snapshot to persistent storage
	rf.persistSnapshotLocked(snapshotData)
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

		if len(rf.peers) == 1 {
			// If we are in a debug scenario with one node, we don't need an election; declare ourselves the winner
			rf.becomeLeaderLocked()
		} else {
			// Otherwise, start a preelection as usual
			rf.startPreVoteLocked()
		}

		rf.mu.Unlock()
	}
}

// startPreVoteLocked runs a pre-election to prevent unnecessary term increases for impossible elections
func (rf *Raft) startPreVoteLocked() {
	rf.preVoteTerm = rf.currentTerm + 1
	rf.preVotesReceived = 1
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.log[rf.logIndexToMemoryIndexLocked(lastLogIndex)].Term

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
	rf.persistLocked()

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
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[rf.logIndexToMemoryIndexLocked(lastLogIndex)].Term
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
		rf.becomeFollowerLocked()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persistLocked()
		
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
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.log[rf.logIndexToMemoryIndexLocked(lastLogIndex)].Term
	if (args.LastLogIndex < lastLogIndex && args.LastLogTerm == lastLogTerm) || args.LastLogTerm < lastLogTerm {
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
		rf.becomeFollowerLocked()
		rf.votedFor = -1
		rf.persistLocked()
	}

	// Ensure the candidate's log is at least as up to date as ours
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.log[rf.logIndexToMemoryIndexLocked(lastLogIndex)].Term
	if (args.LastLogIndex < lastLogIndex && args.LastLogTerm == lastLogTerm) || args.LastLogTerm < lastLogTerm {
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
		rf.persistLocked()
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
		rf.becomeFollowerLocked()
		rf.votedFor = -1
		rf.persistLocked()
	}

	// Destroy candidacy if present, we have a valid leader
	if rf.state != Follower {
		rf.becomeFollowerLocked()
	}

	// Check if we snapshotted something the leader is unaware we snapshotted
	if args.PrevLogIndex < rf.log[0].Index {
		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
			ConflictIndex: rf.log[0].Index + 1,
			ConflictTerm: -1,
		}, true
	}

	// Before addition, check previous to ensure compatibility
	// Log index does not exist
	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		return &AppendEntriesReply{
			Term: rf.currentTerm,
			Success: false,
			ConflictIndex: rf.lastLogIndexLocked() + 1,
			ConflictTerm: -1,
		}, true
	}
	// Index exists, but term does not match
	if args.PrevLogIndex >= 0 && rf.log[rf.logIndexToMemoryIndexLocked(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// Conflict term is the term of this log's value at the index the leader thinks we match
		conflictTerm := rf.log[rf.logIndexToMemoryIndexLocked(args.PrevLogIndex)].Term
		// Must crawl back conflict index to find the first index in the conflict term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.log[0].Index && rf.log[rf.logIndexToMemoryIndexLocked(conflictIndex - 1)].Term == conflictTerm {
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
		if logIndex >= rf.lastLogIndexLocked() {
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
			rf.persistLocked()
			break
		}

		if rf.log[rf.logIndexToMemoryIndexLocked(logIndex)].Term != args.Entries[entryIndex].Term {
			rf.log = rf.log[:rf.logIndexToMemoryIndexLocked(logIndex)]
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
			rf.persistLocked()
			break
		}

		logIndex++
	}

	// Update the commit index
	if args.LeaderCommit > rf.commitIndex {
		last := rf.lastLogIndexLocked()
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

// HandleInstallSnapshot (RPC recipient) handles an install snapshot RPC from another node
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	rf.mu.Lock()

	// Cannot accept snapshot if dead
	if rf.killed {
		rf.mu.Unlock()
		return nil, false
	}

	// If the snapshot is too old, we reject and return our term
	if args.Term < rf.currentTerm {
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		return &InstallSnapshotReply{Term: currentTerm}, true
	}

	// If the snapshot is newer than us, update our term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.becomeFollowerLocked()
	rf.resetElectionTimerLocked()
	rf.persistLocked()

	// Ignore useless snapshot
	if args.LastIncludedIndex <= rf.log[0].Index {
		rf.mu.Unlock()
		return &InstallSnapshotReply{Term: rf.currentTerm}, true
	}

	// Discard the log and replace it with the snapshot
	newSentinel := LogEntry{ Term: args.LastIncludedTerm, Index: args.LastIncludedIndex }
	rf.log = []LogEntry{newSentinel}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persistSnapshotLocked(args.Data)
	rf.persistLocked()

	rf.mu.Unlock()

	// Apply the snapshot to the state machine
	rf.applyCh <- ApplyMessage{
		Type: ApplyMessageSnapshot,
		Data: args.Data,
		Index: args.LastIncludedIndex,
		Term: args.LastIncludedTerm,
	}

	return &InstallSnapshotReply{Term: rf.currentTerm}, true
}

// becomeLeader launches goroutines to send heartbeat signals and check replies
// Precondition: mutex locked
func (rf *Raft) becomeLeaderLocked() {

	rf.state = Leader

	// Initialize nextIndex and matchIndex
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for _, peerId := range rf.peers {
		rf.nextIndex[peerId] = rf.lastLogIndexLocked() + 1
		rf.matchIndex[peerId] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndexLocked()

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

// findLastIndexOfTermLocked finds the last log index containing a value in the given term
// Precondition: mutex locked
func (rf *Raft) findLastIndexOfTermLocked(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return rf.log[i].Index
		}
	}
	return -1
}

// advanceCommitIndexLocked advances the commit index to its latest point
// Precondition: mutex locked
func (rf *Raft) advanceCommitIndexLocked() {
	// Search from the end of the log
	for i := rf.lastLogIndexLocked(); i > rf.commitIndex; i-- {
		// Ensure we are processing log data in the correct term
		if rf.log[rf.logIndexToMemoryIndexLocked(i)].Term != rf.currentTerm {
			continue
		}

		// Count how many peers agree with our log
		count := 1
		for _, peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.matchIndex[peer] >= i {
				count++
			}
		}

		// If enough agree, we have committed the log
		// always commit if only one node
		if count > len(rf.peers) / 2 || len(rf.peers) == 1 {
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

	// If we need to send a snapshot, do so instead of sending a typical heartbeat (will catch up on next heartbeat)
	// Snapshot necessary if we deleted log entries that the follower is missing
	if rf.nextIndex[peerId] <= rf.log[0].Index {
		// Make snapshot parameters
		lastIncludedIndex := rf.log[0].Index
		lastIncludedTerm := rf.log[0].Term
		term := rf.currentTerm
		leaderId := rf.me
		rf.mu.Unlock()

		// Send the snapshot over
		snapshotData, err := rf.persister.LoadSnapshot()
		if err != nil {
			panic("snapshot does not exist but should, unable to continue as chain of state is irreversably broken on authoritative leader")
		}

		reply, success := rf.transport.CallInstallSnapshot(peerId, &InstallSnapshotArgs{
			Term: term,
			LeaderId: leaderId,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm: lastIncludedTerm,
			Data: snapshotData,
		})

		// Ignore dead node
		if !success {
			return
		}

		// Process the reply and update the follower's status in our state
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// In scenarios where we no longer are concerned with the response, don't be
		if rf.killed || rf.state != Leader || rf.currentTerm != term {
			return
		}

		// If we are outdated, step down immediately
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomeFollowerLocked()
			rf.votedFor = -1
			rf.persistLocked()
			rf.resetElectionTimerLocked()
			return
		}

		// Otherwise, the follower is up to date to the snapshot now
		rf.matchIndex[peerId] = lastIncludedIndex
		rf.nextIndex[peerId] = lastIncludedIndex + 1

		return
	}

	// Get the log entries that need to be sent
	logsToSend := append([]LogEntry(nil), rf.log[rf.logIndexToMemoryIndexLocked(rf.nextIndex[peerId]):]...) // make copy
	// Deep copy byte arrays to ensure each node gets a fresh copy of each LogEntry
	for i := range logsToSend {
		logsToSend[i].Command = append([]byte(nil), logsToSend[i].Command...)
	}
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := rf.log[rf.logIndexToMemoryIndexLocked(prevLogIndex)].Term

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
		rf.becomeFollowerLocked()
		rf.votedFor = -1
		rf.persistLocked()
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
			last := rf.findLastIndexOfTermLocked(reply.ConflictTerm)
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
	rf.state = Follower // do not call becomeFollowerLocked, we will run the election timer later

	// Reset volatile state to simulate being rebooted
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	// Restore persistent state using the persister
	state, err := rf.persister.Load()
	if err != nil {
		panic("could not load persistent state when reviving node")
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log

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
func (rf *Raft) GetApplyChannel() (chan ApplyMessage) {
	return rf.applyCh
}

func (rf *Raft) GetLogSizeSinceSnapshot() int {
	return len(rf.log) - 1
}
