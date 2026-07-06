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

func (n *FakeNetwork) CallRequestVote(from, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	// TODO: simulate delay

	// check reachability
	reachable, ok := n.reachable[[2]int{from, to}]
	// reachable by default
	if !ok {
		n.reachable[[2]int{from, to}] = true
		reachable = true
	}
	// if not reachable, fail out
	if !reachable {
		return nil, false
	}

	node, ok := n.nodes[to]
	if !ok {
		return nil, false
	}

	return node.HandleRequestVote(args)
}