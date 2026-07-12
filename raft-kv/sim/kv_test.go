package sim

import "github.com/michaelrothkopf/my-raft-implementation/raft-kv/kv"

// Creates a new cluster and associated client for testing key-value store
func NewKeyValueTestClient(n int) *kv.Client {
	_, _, _, client := NewKeyValueTestClientFull(n)
	return client
}

// Same as NewKeyValueTestClient but returns the cluster, network, and transport as well
func NewKeyValueTestClientFull(n int) (*TestCluster, *FakeKeyValueNetwork, *FakeKeyValueNetworkTransport, *kv.Client) {
	tc := NewTestCluster(n)
	kvn := NewFakeKeyValueNetworkFromTestCluster(tc)
	kvt := NewFakeKeyValueNetworkTransport(kvn)
	return tc, kvn, kvt, kv.NewClient(0, kvt)
}
