package sim

import (
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

// leaders returns all of the nodes who currently believe that they are the leader
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
