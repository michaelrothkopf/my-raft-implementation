package sim

import (
	"bytes"
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
		node := raft.NewRaft(id, ids, transport, 0, -1, nil)
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

// TestBasicAgreement tests that a cluster of nodes under ideal conditions replicates the messages exactly and in the correct order
func TestBasicAgreement(t *testing.T) {
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
				if !bytes.Equal(message.Command, commands[i]) {
					t.Fatalf("expected message \"%s\" at index %d but got \"%s\" instead", commands[i], i, message.Command)
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
			if !bytes.Equal(message.Command, commands[i]) {
				t.Fatalf("previously separated node did not have expected command %s at index %d, had %s instead", commands[i], i, message.Command)
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
			if !bytes.Equal(message.Command, commands[i]) {
				t.Fatalf("previously dead node did not have expected command %s at index %d, had %s instead", commands[i], i, message.Command)
			}
		// Timeout case
		case <-time.After(1 * time.Second):
			t.Errorf("previously dead node did not receive any commands (error occurred on index %d)", i)
		}
	}
}