# Testing

I used Go's built in testing framework to test this project. A more ambitious build could have used more advanced testing software and networking testing, but I chose a simpler but still rigorous framework for my first project using Go and my first time working with any sort of consensus algorithm. There are many more tests to test the Raft protocol than the key-value store, and that is intentional; this project is primarily a Raft implementation and secondarily a key-value store. The key-value store layer, furthermore, is very simple and almost behaves as a cache layer, acting as the state machine for the Raft network.

## Test Inventory

I implemented a variety of tests for the Raft protocol and the key-value storage system.

#### raft/raft_test.go
| Name | Validation |
| --- | --- |
| TestGetRandomElectionTimeout | GetRandomElectionTimeout function returns values within the range |
| TestHandleRequestVote_RejectsStaleTerm | Node does not vote for outdated peer |
| TestHandleRequestVote_RejectsKilled | Node does not vote for dead peer |
| TestHandleRequestVote_RejectsNonFirstVoteInTerm | Node does not vote twice |
| TestHandleRequestVote_GrantsFirstVoteInTerm | Node grants a vote in the current term if it has not voted |
| TestHandleRequestVote_GrantsFirstVoteInHigherTerm | Node grants a vote in a higher term if it has not voted |
| TestHandleRequestVote_GrantsNonFirstVoteInHigherTerm | Node grants a vote in a higher term even if it already voted |
| TestHandleRequestPreVote_RejectsKilled | Dead node does not prevote |
| TestHandleRequestPreVote_RejectsOlderTerm | Node will not grant prevote for outdated candidate |
| TestHandleRequestPreVote_RejectsOutOfDateLogByTerm | Node will not grant prevote for a candidate behind in the log (by term) |
| TestHandleRequestPreVote_RejectsOutOfDateLogByIndex | Node will not grant prevote for a candidate behind in the log (by index) |
| TestHandleRequestPreVote_GrantsNewerTerm | Node will grant prevote for a newer term |
| TestHandleRequestPreVote_GrantsGoodLogLength | Node will grant prevote for a valid log (border case) |
| TestHandleAppendEntries_RejectsKilled | Dead node will not respond to append entries | 
| TestHandleAppendEntries_FailsOutdatedTerm | Node will not accept out-of-date node as leader |
| TestHandleAppendEntries_SubmitsToNewerTerm | Node will submit to a new leader in a newer term |
| TestHandleAppendEntries_FailsWithOwnLogMissingIndex | Node will fail if it is missing a log from the past |
| TestHandleAppendEntries_FailsWithTermMismatch | Node will fail if the terms do not match (receiving node is outdated) |
| TestHandleAppendEntries_SuceedsWithValidNewEntries | Node will succeed if all conditions are met |

#### sim/cluster_test.go
| Name | Validation |
| --- | --- |
| TestInitialElection | Fresh cluster elects exactly one leader |
| TestReelection | Cluster reelects leader after death |
| TestPartitionReelection | Cluster manages a partition by electing a leader only in the larger (majority) partition |
| TestBasicLogReplication | Cluster replicates a log entry |
| TestFollowerPropagationPostPartition | Cluster recovers from a partition by replicating forgotten entries after healing |
| TestFollowerPropagationPostRevive | Cluster recovers from a dead node by replicating foreign entries after reviving |
| TestPersistenceTransferWithFreshNode | Node transfers persistence to a fresh node |
| TestSnapshotCapture | Node captures a snapshot |
| TestSnapshotPropagationPostPartition | Snapshots propagate to lost nodes following a partition healing |
| TestSnapshotPropagationPostRevive | Snapshots propagate to lost nodes following a killing and revival |
| TestSingleNodeCluster | A cluster with only one node functions as intended, critical for testing key-value store |

#### sim/kv_test.go
| Name | Validation |
| --- | --- |
| TestKeyValueSetGet | Setting and getting work |
| TestKeyValueLeaderFailure | Sets and deletes propagate post failure |
| TestKeyValueRepeatRefusal | Duplicates are not doubly executed |
| TestKeyValueDelete | Delete operation works |

## Bugs Found during Testing

The largest and most important bug I found was the PreVote bug. Please see [the README](../README.md) for information about that bug.

### Network Registration

When I first implemented the networking, I made multiple errors with registration. I had never tried to implement an object-oriented program using factory functions like this before, so it was a challenge to get the parameters and deep copies correct. (I've used C before, but never to make anything this complicated. The other C family languages I've worked with have constructors.)

Later on, when I refactored my constructor to read from the persister, I also encountered various errors with registration. Since all of my previous tests were written for nonpersistent nodes, I had to write a helper "backdoor" internal method to allow the tests to run. There was no reason to rewrite all of the tests, since the functionality of the Raft node components that these tests were testing were not modified at all by the implementation of persistence.

### Double-Mutex

I had also never worked with mutexes before (this is also my first project that uses any type of parallel programming) so I encountered many subtle bugs with these throughout development. One such bug occurred with calling functions from within and not from within goroutines. I was unaware of how Go prescribed locked vs unlocked methods to allow mutations to occur from already locked functions, so I made many calls to disallowed methods because the mutex was already locked.

To fix these errors, I duplicated many utility (private) methods to provide locked and unlocked versions. This stablized my control flow and allowed my existing functionality to keep working as I uncovered errors later down the road.

### Heartbeat Mutex Leak

Again due to my inexperience with mutexes, I let up a some bugs in the heartbeat send methods in the Raft server. When I originally sent heartbeats out, I was performing unsafe reads from a goroutine that checked whether the node should still send a heartbeat out or not. The check occurred under an unlocked mutex, so the node could lose leadership and send a false `AppendEntries`. Thus, I restructured my heartbeat code to contain a helper function that locks the mutex before it actually sends the message, ensuring state is preserved without unnecessary mutex locks.
