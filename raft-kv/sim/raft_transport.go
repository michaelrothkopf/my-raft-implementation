package sim

import "github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"

// FakeRaftNetworkTransport implements raft.RPCTransport
// no mutex; all state is immutable
type FakeRaftNetworkTransport struct {
	network		*FakeRaftNetwork
	me			int
}

// NewFakeRaftNetworkTransport constructs a FakeRaftNetworkTransport
func NewFakeRaftNetworkTransport(network *FakeRaftNetwork, me int) *FakeRaftNetworkTransport {
	nt := &FakeRaftNetworkTransport{ network: network, me: me }
	return nt
}

// CallRequestVote passes through to the network using own id as from
func (nt *FakeRaftNetworkTransport) CallRequestVote(peerId int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, bool) {
	return nt.network.CallRequestVote(nt.me, peerId, args)
}

// CallAppendEntries passes through to the network using own id as from
func (nt *FakeRaftNetworkTransport) CallAppendEntries(peerId int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, bool) {
	return nt.network.CallAppendEntries(nt.me, peerId, args)
}

// CallRequestPreVote passes through to the network using own id as from
func (nt *FakeRaftNetworkTransport) CallRequestPreVote(peerId int, args *raft.RequestPreVoteArgs) (*raft.RequestPreVoteReply, bool) {
	return nt.network.CallRequestPreVote(nt.me, peerId, args)
}

func (nt *FakeRaftNetworkTransport) CallInstallSnapshot(peerId int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, bool) {
	return nt.network.CallInstallSnapshot(nt.me, peerId, args)
}
