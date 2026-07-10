package sim

import (
	"sync"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"
)

type FakeRaftNetwork struct {
	mu			sync.Mutex
	nodes		map[int]*raft.Raft
	reachable	map[[2]int]bool // (from, to) -> isReachable
}

// NewFakeRaftNetwork constructs a FakeNetwork
func NewFakeRaftNetwork() *FakeRaftNetwork {
	return &FakeRaftNetwork{
		nodes:		make(map[int]*raft.Raft),
		reachable:	make(map[[2]int]bool),
	}
}

// RegisterNode adds a new node
func (n *FakeRaftNetwork) RegisterNode(id int, node *raft.Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

// SetReachable modifies whether a specific connection is allowed
func (n *FakeRaftNetwork) SetReachable(from, to int, reachable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reachable[[2]int{from, to}] = reachable
}

// Partition groups nodes by id such that no nodes from A may communicate with B and vice versa
func (n *FakeRaftNetwork) Partition(groupA, groupB []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			n.reachable[[2]int{a, b}] = false
			n.reachable[[2]int{b, a}] = false
		}
	}
}

// Heal unpartitions nodes such that all nodes may once again communicate
func (n *FakeRaftNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reachable = make(map[[2]int]bool)
}

// isReachableLocked determines whether a node is reachable
// Precondition: mutex locked
func (n *FakeRaftNetwork) isReachableLocked(from, to int) bool {
	reachable, ok := n.reachable[[2]int{from, to}]
	// Set reachable by default
	if !ok {
		n.reachable[[2]int{from, to}] = true
		reachable = true
	}
	return reachable
}

// isAllowed determines whether a node is reachable and exists
func (n *FakeRaftNetwork) isAllowed(from, to int) (*raft.Raft, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	reachable := n.isReachableLocked(from, to)
	node, ok := n.nodes[to]
	return node, (reachable && ok)
}

// CallRPC generically executes an RPC from one node to another
func callRpc[Args any, Reply any](n *FakeRaftNetwork, from, to int, args *Args, handler func(*raft.Raft, *Args) (*Reply, bool)) (*Reply, bool) {
	// TODO: simulate delay
	
	// Check availability
	node, allowed := n.isAllowed(from, to)
	if !allowed {
		return nil, false
	}

	// Pass through
	return handler(node, args)
}

// CallRequestVote passes a RequestVote RPC through
func (n *FakeRaftNetwork) CallRequestVote(from, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	return callRpc(n, from, to, args, (*raft.Raft).HandleRequestVote)
}

// CallAppendEntries passes an AppendEntries RPC through
func (n *FakeRaftNetwork) CallAppendEntries(from, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	return callRpc(n, from, to, args, (*raft.Raft).HandleAppendEntries)
}

// CallRequestPreVote passes a RequestPreVote RPC through
func (n *FakeRaftNetwork) CallRequestPreVote(from, to int, args *raft.RequestPreVoteArgs) (*raft.RequestPreVoteReply, bool) {
	return callRpc(n, from, to, args, (*raft.Raft).HandleRequestPreVote)
}

// CallInstallSnapshot passes an InstallSnapshot RPC through
func (n *FakeRaftNetwork) CallInstallSnapshot(from, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, bool) {
	return callRpc(n, from, to, args, (*raft.Raft).HandleInstallSnapshot)
}
