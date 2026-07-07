package sim

import (
	"sync"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"
)

type FakeNetwork struct {
	mu			sync.Mutex
	nodes		map[int]*raft.Raft
	reachable	map[[2]int]bool // (from, to) -> isReachable
}

// NewFakeNetwork constructs a FakeNetwork
func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		nodes:		make(map[int]*raft.Raft),
		reachable:	make(map[[2]int]bool),
	}
}

// RegisterNode adds a new node
func (n *FakeNetwork) RegisterNode(id int, node *raft.Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

// SetReachable modifies whether a specific connection is allowed
func (n *FakeNetwork) SetReachable(from, to int, reachable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reachable[[2]int{from, to}] = reachable
}

// Partition groups nodes by id such that no nodes from A may communicate with B and vice versa
func (n *FakeNetwork) Partition(groupA, groupB []int) {
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
func (n *FakeNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reachable = make(map[[2]int]bool)
}

// isReachableLocked determines whether a node is reachable
func (n *FakeNetwork) isReachableLocked(from, to int) bool {
	reachable, ok := n.reachable[[2]int{from, to}]
	// Set reachable by default
	if !ok {
		n.reachable[[2]int{from, to}] = true
		reachable = true
	}
	return reachable
}

// CallRequestVote passes a RequestVote RPC through
func (n *FakeNetwork) CallRequestVote(from, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	// TODO: simulate delay
	
	// Check availability
	n.mu.Lock()
	reachable := n.isReachableLocked(from, to)
	node, ok := n.nodes[to]
	n.mu.Unlock()
	if !reachable || !ok {
		return nil, false
	}

	// Pass through
	return node.HandleRequestVote(args)
}

// CallAppendEntries passes an AppendEntries RPC through
func (n *FakeNetwork) CallAppendEntries(from, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	// TODO: simulate delay
	
	// Check availability
	n.mu.Lock()
	reachable := n.isReachableLocked(from, to)
	node, ok := n.nodes[to]
	n.mu.Unlock()
	if !reachable || !ok {
		return nil, false
	}

	// Pass through
	return node.HandleAppendEntries(args)
}