# My Raft Implementation

A simple implementation of the Raft protocol for consensus among distribtued server nodes.

This implementation uses Go. I'm trying to avoid third-party libraries outside of the standard library.

## Structure

- 📂 `raft-kv/` Primary project code folder
    - 📂 `raft/` Raft implementation and tests (independent of newtorking protocol)
        - 📄 `raft.go` Implementations for the Raft logic
        - 📄 `rpc.go` Interfaces for RPC transport layer to implement
    - 📂 `sim/` Simulated network for early testing
        - 📄 `network.go` A simple fake network handler to manage node communication
        - 📄 `transport.go` A fake network compatible RPC implementation to interface with Raft
        - 📄 `cluster_test.go` Testing for the simulated network

## Development

I decided to make this project to learn more about consensus and distributed networks as I actively research distributed LLMs and hosting with the First State AI Institute at the University of Delaware.

I close-read the [Raft paper](https://raft.github.io/raft.pdf) and some supplemental materials before I designed my programm. I also looked into Paxos to understand the motivation behind Raft.
