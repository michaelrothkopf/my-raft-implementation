package sim

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"
)

// testCluster is a simple cluster of nodes in the network
type testCluster struct {
	network		*FakeNetwork
	nodes		map[int]*raft.Raft
	ids			[]int
}

// newTestCluster constructs a fresh TestCluster for a test
func newTestCluster(n int) *testCluster {
	network := NewFakeNetwork()
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		ids[i] = i
	}

	nodes := make(map[int]*raft.Raft)
	for _, id := range ids {
		transport := NewFakeNetworkTransport(network, id)
		node := raft.NewRaftWithoutReadingFromPersistence(id, ids, transport, 0, -1, nil, raft.NewMemoryPersister())
		nodes[id] = node
		network.RegisterNode(id, node)
	}

	return &testCluster{
		network: network,
		nodes: nodes,
		ids: ids,
	}
}

// leaders returns the ids of all of the nodes who currently believe that they are the leader
func (tc *testCluster) leaders() []int {
	var result []int
	for id, node := range tc.nodes {
		if node.GetState() == raft.Leader {
			result = append(result, id)
		}
	}
	return result
}

// TestInitialElection tests the election on a fresh cluster
func TestInitialElection(t *testing.T) {
	tc := newTestCluster(3)

	// Allow the cluster to elect a new leader
	time.Sleep(1 * time.Second)

	leaders := tc.leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly one leader, got %d: %v", len(leaders), leaders)
	}

	leaderTerm := tc.nodes[leaders[0]].GetCurrentTerm()
	for _, id := range tc.ids {
		nodeTerm := tc.nodes[id].GetCurrentTerm()
		if nodeTerm != leaderTerm {
			t.Fatalf("node %d has term %d, but leader has term %d", id, nodeTerm, leaderTerm)
		}
	}
}

// TestReelection tests the election after the leader has been killed
func TestReelection(t *testing.T) {
	tc := newTestCluster(4)

	// Allow the cluster to elect a leader
	time.Sleep(1 * time.Second)
	
	// Kill the leader
	leaders := tc.leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly one leader, got %d: %v", len(leaders), leaders)
	}
	leader := leaders[0]
	tc.network.nodes[leader].Kill()

	// Allow the cluster to elect a new leader
	time.Sleep(1 * time.Second)
	
	// Ensure there is exactly one leader
	newLeaders := tc.leaders()
	if len(newLeaders) != 1 {
		t.Fatalf("expected exactly one leader, got %d: %v", len(newLeaders), newLeaders)
	}
	// Leader must be different
	if newLeaders[0] == leader {
		t.Fatalf("new leader must differ from old leader after killing old leader")
	}
}

// TestPartitionReelection tests that the majority partition successfully reelects a leader but the minority doesn't
func TestPartitionReelection(t *testing.T) {
	tc := newTestCluster(5)

	// Allow the cluster to elect a leader
	time.Sleep(1 * time.Second)

	// Ensure there is a leader
	unpartitionedLeaders := tc.leaders()
	if len(unpartitionedLeaders) != 1 {
		t.Fatalf("expected exactly one leader, got %d: %v", len(unpartitionedLeaders), unpartitionedLeaders)
	}
	oldLeader := unpartitionedLeaders[0]

	// Partition the network
	groupA := []int{0, 1}
	groupB := []int{2, 3, 4}
	tc.network.Partition(groupA, groupB)

	// Give time to notice and reelect
	time.Sleep(1 * time.Second)

	// Make sure the majority has a one leader
	majorityLeaders, minorityLeaders := 0, 0
	for _, id := range tc.leaders() {
		if slices.Contains(groupA, id) {
			minorityLeaders++
		} else {
			majorityLeaders++
		}
	}
	if majorityLeaders != 1 {
		t.Fatalf("expected exactly one leader in majority, got %d", majorityLeaders)
	}
	// Expect leader in minority if old leader was minority; else, expect 0
	if slices.Contains(groupA, oldLeader) {
		if minorityLeaders != 1 {
			t.Fatalf("expected old leader in minority but stepped down unexpectedly, got %d leaders", minorityLeaders)
		}
	} else if minorityLeaders != 0 {
		t.Fatalf("expected no leaders in minority partition, got %d", minorityLeaders)
	}
}

// TestBasicLogReplication tests that a cluster of nodes under ideal conditions replicates the messages exactly and in the correct order
func TestBasicLogReplication(t *testing.T) {
	tc := newTestCluster(5)

	// Allow leader selection
	time.Sleep(1 * time.Second)

	// Find the leader
	leaders := tc.leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly one leader, got %d: %v", len(leaders), leaders)
	}
	leader := tc.nodes[leaders[0]]

	// Ensure the leader accepts the message
	index, _, isLeader := leader.Start([]byte("Go Your Own Way")) // idx 0
	if !isLeader {
		t.Fatalf("leader node is not leader; did not accept command")
	}
	if index != 1 {
		t.Fatalf("command not indexed as first command (sentinel is 0, index should be 1, got index is %d)", index)
	}

	// Send some messages to the leader
	commands := [][]byte{
		[]byte("Go Your Own Way"), // duplicate for easier checking code; does not get double added
		[]byte("Say That You Love Me"),
		[]byte("Dreams"),
		[]byte("The Chain"),
		[]byte("Landslide"),
		[]byte("Silver Springs"),
	}
	for id, command := range commands {
		if id == 0 {
			continue
		}
		leader.Start(command)
	}

	// Allow the messages to propagate
	time.Sleep(500 * time.Millisecond)

	// Drain applyCh for each node to ensure parity
	for id := range tc.ids {
		for i := range commands {
			select {
			case message := <-tc.nodes[id].GetApplyChannel():
				if !bytes.Equal(message.Data, commands[i]) {
					t.Fatalf("expected message \"%s\" at index %d but got \"%s\" instead", commands[i], i, message.Data)
				}
			// Timeout case
			case <-time.After(1 * time.Second):
				t.Errorf("node %d never applied command index %d", id, i)
			}
		}
	}
}

// TestFollowerPropagationPostPartition tests that a previously partitioned node successfully receives messages that were sent while it slept
func TestFollowerPropagationPostPartition(t *testing.T) {
	tc := newTestCluster(3)

	// Allow it to select a leader
	time.Sleep(1 * time.Second)

	// Partition a follower away
	groupA, groupB := []int{0, 1}, []int{2}
	tc.network.Partition(groupA, groupB)

	// Allow a new leader to be selected
	time.Sleep(1 * time.Second)

	// Get the new leader
	leaders := tc.leaders()
	// Must ensure leaderId is from larger partition (remains leaderId, commands will not be dropped)
	var leaderId int
	if len(leaders) > 1 && leaders[0] == 2 {
		leaderId = leaders[1]
	} else {
		leaderId = leaders[0]
	}
	if leaderId == 2 {
		t.Fatalf("only leader is in small partition")
	}

	// Send some commands
	commands := [][]byte{
		[]byte("Zanzibar"),
		[]byte("Stiletto"),
		[]byte("52nd Street"),
		[]byte("A Matter of Trust"),
		[]byte("Goodnight Saigon"),
	}
	for _, command := range commands {
		tc.nodes[leaderId].Start(command)
	}

	// Allow them to propagate
	time.Sleep(500 * time.Millisecond)

	// Heal the network
	tc.network.Heal()

	// Allow them to propagate
	time.Sleep(500 * time.Millisecond)

	// Ensure node 2 has the commands in the correct order
	for i := range commands {
		select {
		case message := <-tc.nodes[2].GetApplyChannel():
			if !bytes.Equal(message.Data, commands[i]) {
				t.Fatalf("previously separated node did not have expected command %s at index %d, had %s instead", commands[i], i, message.Data)
			}
		// Timeout case
		case <-time.After(1 * time.Second):
			t.Errorf("previously separated node did not receive any commands (error occurred on index %d)", i)
		}
	}
}

func TestFollowerPropagationPostRevive(t *testing.T) {
	tc := newTestCluster(3)

	// Allow it to select a leader
	time.Sleep(1 * time.Second)

	// Kill someone
	tc.nodes[2].Kill()

	// Allow a new leader to be selected
	time.Sleep(1 * time.Second)

	// Get the new leader
	leaders := tc.leaders()
	// Should be only one leader
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader, got %d (%v)", len(leaders), leaders)
	}
	// Get the leader
	leaderId := leaders[0]

	// Send some commands
	commands := [][]byte{
		[]byte("Zanzibar"),
		[]byte("Stiletto"),
		[]byte("52nd Street"),
		[]byte("A Matter of Trust"),
		[]byte("Goodnight Saigon"),
	}
	for _, command := range commands {
		tc.nodes[leaderId].Start(command)
	}
	
	// Allow the commands to propagate
	time.Sleep(500 * time.Millisecond)

	// Revive the dead
	tc.nodes[2].Revive()

	// Allow the commands to propagate
	time.Sleep(500 * time.Millisecond)

	// Ensure node 2 has the commands in the correct order
	for i := range commands {
		select {
		case message := <-tc.nodes[2].GetApplyChannel():
			if !bytes.Equal(message.Data, commands[i]) {
				t.Fatalf("previously dead node did not have expected command %s at index %d, had %s instead", commands[i], i, message.Data)
			}
		// Timeout case
		case <-time.After(1 * time.Second):
			t.Errorf("previously dead node did not receive any commands (error occurred on index %d)", i)
		}
	}
}

func TestPersistenceTransferWithFreshNode(t *testing.T) {
	persister := raft.NewMemoryPersister()
	network := NewFakeNetwork()
	ids := []int{0, 1, 2}
	
	transport := NewFakeNetworkTransport(network, 0)
	node := raft.NewRaft(0, ids, transport, persister)
	network.RegisterNode(0, node)

	// Cast a vote to persist some state
	node.HandleRequestVote(&raft.RequestVoteArgs{
		Term: 5, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0,
	})

	// Create a new node with the same persistence
	newNode := raft.NewRaft(0, ids, transport, persister)

	// Ensure the persistent state transferred
	// Term matches
	if newNode.GetCurrentTerm() != 5 {
		t.Fatalf("new node did not receive persistent data")
	}
	// Will not vote again
	reply, _ := newNode.HandleRequestVote(&raft.RequestVoteArgs{
		Term: 5, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if reply.VoteGranted {
		t.Fatalf("new node voted without sentinel votedFor")
	}
}

func TestSnapshotCapture(t *testing.T) {
	persister := raft.NewMemoryPersister()
	network := NewFakeNetwork()
	ids := []int{0}
	
	transport := NewFakeNetworkTransport(network, 0)
	node := raft.NewRaft(0, ids, transport, persister)
	network.RegisterNode(0, node)

	// Allow it to select itself as leader
	time.Sleep(1 * time.Second)

	if node.GetState() != raft.Leader {
		t.Fatalf("node did not elect itself leader")
	}

	// Write some commands to the log
	commands := [][]byte{
		[]byte("Rainy Day People"),
		[]byte("Talking In Your Sleep"),
		[]byte("Beautiful"),
		[]byte("Me and Bobby McGee"),
		[]byte("If You Could Read My Mind"),
	}
	for _, command := range commands {
		node.Start(command)
	}

	// Allow the commands to commit
	time.Sleep(500 * time.Millisecond)

	// Snapshot the commands
	snapshotData := []byte("Snapshot Test Data")
	node.Snapshot(5, snapshotData)

	// Allow the snapshot to take effect
	time.Sleep(500 * time.Millisecond)

	// Ensure the data has been saved to the persister
	actualSnapshotData, err := persister.LoadSnapshot()
	if err != nil {
		t.Fatalf("unable to load snapshot")
	}
	if !bytes.Equal(actualSnapshotData, snapshotData) {
		t.Fatalf("snapshot data was not the data that was passed in (actual: %v)", actualSnapshotData)
	}
}

func TestSnapshotPropagationPostPartition(t *testing.T) {
	tc := newTestCluster(3)

	// Substitute a node to extract its persister data
	tc.network.nodes[2].Kill()
	persister := raft.NewMemoryPersister()
	tc.nodes[2] = raft.NewRaftWithoutReadingFromPersistence(2, tc.ids, NewFakeNetworkTransport(tc.network, 2), 0, -1, nil, persister)
	tc.network.nodes[2] = tc.nodes[2]

	// Allow it to select a leader
	time.Sleep(1 * time.Second)

	// Partition a follower away
	groupA, groupB := []int{0, 1}, []int{2}
	tc.network.Partition(groupA, groupB)

	// Allow a new leader to be selected
	time.Sleep(1 * time.Second)

	// Get the new leader
	leaders := tc.leaders()
	// Must ensure leaderId is from larger partition (remains leaderId, commands will not be dropped)
	var leaderId int
	if len(leaders) > 1 && leaders[0] == 2 {
		leaderId = leaders[1]
	} else {
		leaderId = leaders[0]
	}
	if leaderId == 2 {
		t.Fatalf("only leader is in small partition")
	}

	// Send some commands
	commands := [][]byte{
		[]byte("Panama"),
		[]byte("You Really Got Me"),
		[]byte("Dance the Night Away"),
		[]byte("Unchained"),
		[]byte("Ain't Talkin' 'Bout Love"),
	}
	for _, command := range commands {
		tc.nodes[leaderId].Start(command)
	}

	// Allow them to propagate
	time.Sleep(500 * time.Millisecond)

	// Snapshot the old data
	snapshotData := []byte("Snapshot Test Data")
	tc.nodes[leaderId].Snapshot(5, snapshotData)

	// Allow the snapshot to propagate
	time.Sleep(500 * time.Millisecond)

	// Heal the network
	tc.network.Heal()

	// Allow the snapshot to propagate
	time.Sleep(500 * time.Millisecond)

	// Ensure that the snapshot data passed
	actualSnapshotData, err := persister.LoadSnapshot()
	if err != nil {
		t.Fatalf("unable to load snapshot")
	}
	if !bytes.Equal(actualSnapshotData, snapshotData) {
		t.Fatalf("snapshot data was not the data that was passed in (actual: %v)", actualSnapshotData)
	}
}

func TestSnapshotPropagationPostRevive(t *testing.T) {
	tc := newTestCluster(3)

	// Substitute a node to extract its persister data
	tc.network.nodes[2].Kill()
	persister := raft.NewMemoryPersister()
	tc.nodes[2] = raft.NewRaftWithoutReadingFromPersistence(2, tc.ids, NewFakeNetworkTransport(tc.network, 2), 0, -1, nil, persister)
	tc.network.nodes[2] = tc.nodes[2]

	// Allow it to select a leader
	time.Sleep(1 * time.Second)

	// Kill the third node
	tc.nodes[2].Kill()

	// Allow a new leader to be selected
	time.Sleep(1 * time.Second)

	// Get the new leader
	leaders := tc.leaders()
	// Verify leader exists
	if len(leaders) != 1 {
		for i, node := range tc.nodes {
			fmt.Printf("node %d state: %d\n", i, node.GetState())
		}
		t.Fatalf("expected exactly one leader after death of third node, got %d (%v)", len(leaders), leaders)
	}
	leaderId := leaders[0]

	// Send some commands
	commands := [][]byte{
		[]byte("Panama"),
		[]byte("You Really Got Me"),
		[]byte("Dance the Night Away"),
		[]byte("Unchained"),
		[]byte("Ain't Talkin' 'Bout Love"),
	}
	for _, command := range commands {
		tc.nodes[leaderId].Start(command)
	}

	// Allow them to propagate
	time.Sleep(500 * time.Millisecond)

	// Snapshot the old data
	snapshotData := []byte("Snapshot Test Data")
	tc.nodes[leaderId].Snapshot(5, snapshotData)

	// Allow the snapshot to propagate
	time.Sleep(500 * time.Millisecond)

	// Revive the node
	tc.nodes[2].Revive()

	// Allow the snapshot to propagate
	time.Sleep(500 * time.Millisecond)

	// Ensure that the snapshot data passed
	actualSnapshotData, err := persister.LoadSnapshot()
	if err != nil {
		t.Fatalf("unable to load snapshot")
	}
	if !bytes.Equal(actualSnapshotData, snapshotData) {
		t.Fatalf("snapshot data was not the data that was passed in (actual: %v)", actualSnapshotData)
	}
}
