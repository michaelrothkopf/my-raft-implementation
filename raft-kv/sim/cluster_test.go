package sim

import (
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