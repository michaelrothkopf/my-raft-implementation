package sim

import (
	"testing"
	"time"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/kv"
)

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

// TestKeyValueSetGet tests basic getting and setting for a key value cluster of 5 nodes
func TestKeyValueSetGet(t *testing.T) {
	cl := NewKeyValueTestClient(5)

	time.Sleep(500 * time.Millisecond)

	setReply := cl.Set("Michael", "19")
	if setReply.Err != nil {
		t.Fatalf("received error during set: %v", setReply.Err.Error())
	}

	time.Sleep(500 * time.Millisecond)

	getReply := cl.Get("Michael")
	if getReply.Err != nil {
		t.Fatalf("recevied error during get: %v", getReply.Err.Error())
	}
	if !getReply.Exists {
		t.Fatalf("key does not exist")
	}
	if getReply.Value != "19" {
		t.Fatalf("incorrect value: expected \"19\", got \"%v\"", getReply.Value)
	}
}

// TestKeyValueLeaderFailure tests whether the key value server persists when the leader fails
func TestKeyValueLeaderFailure(t *testing.T) {
	tc, _, _, cl := NewKeyValueTestClientFull(5)

	// Allow leader selection
	time.Sleep(500 * time.Millisecond)

	// Get the leader
	leaders := tc.leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader, got %v (%v)", len(leaders), leaders)
	}
	leader := tc.network.nodes[leaders[0]]

	// Set a key
	cl.Set("test1", "answer1")

	// Wait
	time.Sleep(500 * time.Millisecond)

	// Kill the leader
	leader.Kill()

	// Allow leader selection
	time.Sleep(500 * time.Millisecond)

	// Send another key value
	cl.Set("test2", "answer2")

	// Wait
	time.Sleep(500 * time.Millisecond)
	
	// Revive the old leader
	leader.Revive()

	// Wait
	time.Sleep(500 * time.Millisecond)

	// Check both keys
	reply1 := cl.Get("test1")
	if reply1.Err != nil {
		t.Fatalf("recevied error during get reply1: %v", reply1.Err.Error())
	}
	if !reply1.Exists {
		t.Fatalf("key does not exist reply1")
	}
	if reply1.Value != "answer1" {
		t.Fatalf("incorrect value reply1: expected \"answer1\", got \"%v\"", reply1.Value)
	}
	reply2 := cl.Get("test2")
	if reply2.Err != nil {
		t.Fatalf("recevied error during get reply2: %v", reply2.Err.Error())
	}
	if !reply2.Exists {
		t.Fatalf("key does not exist reply2")
	}
	if reply2.Value != "answer2" {
		t.Fatalf("incorrect value reply2: expected \"answer2\", got \"%v\"", reply2.Value)
	}
}

// TestKeyValueRepeatRefusal ensures that the store is not manipulated on a duplicated request
func TestKeyValueRepeatRefusal(t *testing.T) {
	// Extract the transport for direct testing
	_, _, kvt, _ := NewKeyValueTestClientFull(1)

	time.Sleep(500 * time.Millisecond)

	// We are only using one node, so we can assume the leader is 0
	// Send two duplicate commands (same RequestId and ClientId) and ensure that the get returns the original value (command is not applied)
	reply, _ := kvt.CallSet(0, &kv.SetArgs{
		Key: "test",
		Value: "original",
		ClientId: 0,
		RequestId: 0,
	})
	if reply.Err != nil {
		t.Fatalf("failed to set initially with error: %v", reply.Err.Error())
	}
	time.Sleep(500 * time.Millisecond)
	reply, _ = kvt.CallSet(0, &kv.SetArgs{
		Key: "test",
		Value: "new",
		ClientId: 0,
		RequestId: 0,
	})
	if reply.Err != nil {
		t.Fatalf("failed to set initially with error %v", reply.Err.Error())
	}
	// Check the observed result
	time.Sleep(500 * time.Millisecond)
	getReply, _ := kvt.CallGet(0, &kv.GetArgs{
		Key: "test",
		ClientId: 0,
		RequestId: 1,
	})
	if getReply.Err != nil {
		t.Fatalf("failed to complete underlying get request: %v", getReply.Err.Error())
	}
	if !getReply.Exists {
		t.Fatalf("key does not exist upon get")
	}
	if getReply.Value != "original" {
		t.Fatalf("expected \"original\" to be the value of test key, got \"%v\"", getReply.Value)
	}
}

func TestKeyValueDelete(t *testing.T) {
	cl := NewKeyValueTestClient(5)
	
	time.Sleep(500 * time.Millisecond)

	cl.Set("dog", "cat")

	time.Sleep(500 * time.Millisecond)

	result := cl.Get("dog")
	if result.Err != nil {
		t.Fatalf("error during initial get: %v", result.Err.Error())
	}
	if !result.Exists {
		t.Fatalf("key does not exist in initial test")
	}
	if result.Value != "cat" {
		t.Fatalf("expected \"cat\", got \"%s\"", result.Value)
	}

	cl.Delete("dog")

	time.Sleep(500 * time.Millisecond)

	result = cl.Get("dog")
	if result.Exists {
		t.Fatalf("key was not deleted")
	}
}
