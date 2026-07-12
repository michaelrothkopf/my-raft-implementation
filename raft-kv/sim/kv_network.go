package sim

import (
	"errors"
	"slices"
	"strconv"
	"sync"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/kv"
)

type FakeKeyValueNetwork struct {
	mu			sync.Mutex
	nodes		map[int]*kv.KeyValueServer
	ids			[]int
	reachable	map[int]bool
}

func NewFakeKeyValueNetwork() *FakeKeyValueNetwork {
	return &FakeKeyValueNetwork{
		nodes: make(map[int]*kv.KeyValueServer),
		ids: make([]int, 0),
		reachable: make(map[int]bool),
	}
}

func NewFakeKeyValueNetworkFromTestCluster(testCluster *TestCluster) *FakeKeyValueNetwork {
	n := NewFakeKeyValueNetwork()
	for id, node := range testCluster.nodes {
		n.nodes[id] = kv.NewKeyValueServer(node, id)
		n.ids = append(n.ids, id)
		n.reachable[id] = true
	}
	return n
}

func (n *FakeKeyValueNetwork) RegisterNode(id int, node *kv.KeyValueServer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if slices.Contains(n.ids, id) {
		panic("attempting to add node with existing id " + strconv.Itoa(id))
	}
	n.nodes[id] = node
	n.ids = append(n.ids, id)
	n.reachable[id] = true
}

func (n *FakeKeyValueNetwork) SetReachable(id int, reachable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reachable[id] = reachable
}

func (n *FakeKeyValueNetwork) GetReachable(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachable[id]
}

type FakeKeyValueNetworkTransport struct {
	network		*FakeKeyValueNetwork
	currentId	int
}

func NewFakeKeyValueNetworkTransport(network *FakeKeyValueNetwork) *FakeKeyValueNetworkTransport {
	return &FakeKeyValueNetworkTransport{
		network: network,
	}
}

func (nt *FakeKeyValueNetworkTransport) CallSet(serverId int, args *kv.SetArgs) (*kv.SetReply, bool) {
	if !nt.network.GetReachable(serverId) {
		return &kv.SetReply{ Err: errors.New("unable to reach server") }, false
	}
	reply := nt.network.nodes[serverId].Set(args.Key, args.Value, args.ClientId, args.RequestId)
	return &kv.SetReply{ Err: reply }, true
}

func (nt *FakeKeyValueNetworkTransport) CallDelete(serverId int, args *kv.DeleteArgs) (*kv.DeleteReply, bool) {
	if !nt.network.GetReachable(serverId) {
		return &kv.DeleteReply{ Err: errors.New("unable to reach server") }, false
	}
	reply := nt.network.nodes[serverId].Delete(args.Key, args.ClientId, args.RequestId)
	return &kv.DeleteReply{ Err: reply }, true
}

func (nt *FakeKeyValueNetworkTransport) CallGet(serverId int, args *kv.GetArgs) (*kv.GetReply, bool) {
	if !nt.network.GetReachable(serverId) {
		return &kv.GetReply{ Err: errors.New("unable to reach server") }, false
	}
	value, isOk, error := nt.network.nodes[serverId].Get(args.Key)
	return &kv.GetReply{ Value: value, Err: error, Exists: isOk }, true
}

// GetNextServerId cycles through the server IDs one at a time
func (nt *FakeKeyValueNetworkTransport) GetNextServerId(serverId int) int {
	nt.network.mu.Lock()
	defer nt.network.mu.Unlock()
	nt.currentId = (nt.currentId + 1) % len(nt.network.ids)
	return nt.currentId
}
