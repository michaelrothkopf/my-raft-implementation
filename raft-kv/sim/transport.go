package sim

import "github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"

// FakeNetworkTransport implements raft.RPCTransport
// no mutex; all state is immutable
type FakeNetworkTransport struct {
	network		*FakeNetwork
	me			int
}

// NewFakeNetworkTransport constructs a FakeNetworkTransport
func NewFakeNetworkTransport(network *FakeNetwork, me int) *FakeNetworkTransport {
	nt := &FakeNetworkTransport{ network: network, me: me }
	return nt
}

// CallRequestVote passes through to the network using own id as from
func (nt *FakeNetworkTransport) CallRequestVote(peerId int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	return nt.network.CallRequestVote(nt.me, peerId, args)
}

// CallAppendEntries passes through to the network using own id as from
func (nt *FakeNetworkTransport) CallAppendEntries(peerId int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	return nt.network.CallAppendEntries(nt.me, peerId, args)
}