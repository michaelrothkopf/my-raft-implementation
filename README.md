# My Raft Implementation

A simple implementation of the Raft protocol for consensus among distributed server nodes when storing key-value pairs.

This implementation uses Go. I'm trying to avoid third-party libraries outside of the standard library.

## Structure

- 📂 `raft-kv/` Primary project code folder
    - 📂 `raft/` Raft implementation and tests (independent of newtorking protocol)
        - 📄 `raft.go` Implementations for the Raft logic
        - 📄 `rpc.go` Interfaces for RPC transport layer to implement
        - 📄 `raft_test.go` Testing for the core Raft logic
    - 📂 `sim/` Simulated network for early testing
        - 📄 `network.go` A simple fake network handler to manage node communication
        - 📄 `transport.go` A fake network compatible RPC implementation to interface with Raft
        - 📄 `cluster_test.go` Testing for the simulated network

## Development

I decided to make this project to learn more about consensus and distributed networks as I research distributed LLMs and hosting with the First State AI Institute at the University of Delaware.

I close-read the [Raft paper](https://raft.github.io/raft.pdf) and some supplemental materials before I designed my program. I also looked into Paxos to understand the motivation behind Raft.

I am developing this system in stages.
1. Election logic and healing
2. Log replication
3. Persistence and crash recovery
4. Log compaction (snapshotting)
5. KV service API (hopefully REST-based)

### Challenges During Development

My biggest challenge so far occurred during the second stage. As I developed the log replication, I wrote a test called `TestFollowerPropagationPostPartition` that tests the network's healing after a partition.

The test does the following:
1. Create a cluster with 3 nodes
2. Allow the cluster to elect a leader
3. Partition the cluster such that the third node is isolated
4. Allow the majority to elect a new leader if necessary
5. Send some commands
6. Wait for the commands to propagate
7. Unpartition the network
8. Wait for the commands to propagate
9. Check that the third node received all the commands in the correct order

That test failed, not because I had incorrectly written the test or implemented the protocol, but because of a problem that my research concluded was known in other Raft implementations.

The error occurs because if the third node both is not the leader and has sufficient time to attempt to become leader multiple times, it will increment its term beyond the term of the majority nodes. Thus, when the third node rejoins the network, it is newer and overwrites the other nodes' commands.

To fix this, I researched and found `PreVote` (an [extension](https://en.wikipedia.org/wiki/Raft_(algorithm)#Extensions) of Raft), which introduces a pre-candidacy stage. Pre-candidacy allows a `Follower` to ask "could I be elected?" before incrementing its term. Thus, the third node will never enter the candidacy stage and the integrity of the majority is preserved.

I added new structs to the RPC protocol for pre-candidacy:
```go
type RequestPreVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestPreVoteReply struct {
	Term			int
	VoteGranted		bool
}
```

I changed the leader process from `election timer runs out -> start an election` to `election timer runs out -> enter precandidacy to confirm node will win an election -> start an election`.

Following the change, `TestFollowerPropagationPostPartition` succeeded 30 out of 30 times in further testing and is yet to fail due to this error again. I conclude that `PreVote` solved the issue.
