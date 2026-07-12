package sim

import "github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"

// TestCluster is a simple cluster of nodes in the network
type TestCluster struct {
	network		*FakeRaftNetwork
	nodes		map[int]*raft.Raft
	ids			[]int
}

// NewTestCluster constructs a fresh TestCluster for a test
func NewTestCluster(n int) *TestCluster {
	network := NewFakeRaftNetwork()
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		ids[i] = i
	}

	nodes := make(map[int]*raft.Raft)
	for _, id := range ids {
		transport := NewFakeRaftNetworkTransport(network, id)
		node := raft.NewRaftWithoutReadingFromPersistence(id, ids, transport, 0, -1, nil, raft.NewMemoryPersister())
		nodes[id] = node
		network.RegisterNode(id, node)
	}

	return &TestCluster{
		network: network,
		nodes: nodes,
		ids: ids,
	}
}

// leaders returns the ids of all of the nodes who currently believe that they are the leader
func (tc *TestCluster) leaders() []int {
	var result []int
	for id, node := range tc.nodes {
		if node.GetState() == raft.Leader {
			result = append(result, id)
		}
	}
	return result
}