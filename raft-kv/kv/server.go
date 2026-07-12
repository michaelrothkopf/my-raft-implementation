package kv

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/michaelrothkopf/my-raft-implementation/raft-kv/raft"
)

const KeyValueServerSubmitTimeout = 2 * time.Second
const KeyValueServerMaxLogLength = 10

type KeyValueServer struct {
	mu						sync.Mutex
	rf						*raft.Raft
	me						int
	store 					map[string]string

	// Last request applied by each client to prevent repeats
	lastAppliedRequest		map[int]int

	// Channels for notifying clients when their command is applied
	notifyChannels		map[int](chan Command)

	killed bool
}

// NewKeyValueServer constructs a new KeyValueServer
// rf is the pointer to the associated Raft server
// me is the server's ID within the network of other KeyValueServers
func NewKeyValueServer(rf *raft.Raft, me int) *KeyValueServer {
	kv := &KeyValueServer{
		rf: rf,
		me: me,
		store: make(map[string]string),
		lastAppliedRequest: make(map[int]int),
		notifyChannels: make(map[int]chan Command),
	}
	go kv.applyCommandLoop()
	return kv
}

type KeyValueServerSnapshot struct {
	Store				map[string]string
	LastAppliedRequest	map[int]int
}

func (kv *KeyValueServer) Set(key, value string, clientId int, requestId int) error {
	_, err := kv.submit(Command{
		Type: CommandSet,
		Key: key,
		Value: value,
		ClientId: clientId,
		RequestId: requestId,
	})
	return err
}

func (kv *KeyValueServer) Delete(key string, clientId int, requestId int) error {
	_, err := kv.submit(Command{
		Type: CommandDelete,
		Key: key,
		ClientId: clientId,
		RequestId: requestId,
	})
	return err
}

// Get returns (value, isOk, err)
func (kv *KeyValueServer) Get(key string) (string, bool, error) {
	_, err := kv.submit(Command{
		Type: CommandGet,
		Key: key,
	})
	if err != nil {
		return "", false, err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.store[key]
	return value, ok, nil
}

func (kv *KeyValueServer) submit(command Command) (Command, error) {
	data, err := json.Marshal(command)
	if err != nil {
		return Command{}, err
	}

	index, _, isLeader := kv.rf.Start(data)
	if !isLeader {
		return Command{}, ErrNotLeader
	}

	// Get a channel to receive the response
	kv.mu.Lock()
	ch := make(chan Command, 1)
	kv.notifyChannels[index] = ch
	kv.mu.Unlock()

	// Remove the channel at the end of execution
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChannels, index)
		kv.mu.Unlock()
	}()

	select {
	case appliedCommand := <-ch:
		if appliedCommand.ClientId != command.ClientId || appliedCommand.RequestId != command.RequestId {
			return Command{}, ErrNoLongerLeader
		}
		return appliedCommand, nil
	case <-time.After(KeyValueServerSubmitTimeout):
		return Command{}, ErrTimeout
	}
}

func (kv *KeyValueServer) applyCommandLoop() {
	for applyMessage := range kv.rf.GetApplyChannel() {
		if applyMessage.Type == raft.ApplyMessageSnapshot {
			kv.applySnapshot(applyMessage)
			continue
		}

		var command Command
		if err := json.Unmarshal(applyMessage.Data, &command); err != nil {
			panic("corrupted message data encountered when applying command!")
		}

		kv.mu.Lock()
		
		// Ensure the request is not a duplicate
		lastSeenRequestId, havePreviouslySeenRequestsFromClient := kv.lastAppliedRequest[command.ClientId]
		requestIsDuplicate := havePreviouslySeenRequestsFromClient && command.RequestId <= lastSeenRequestId
		// Process the request
		if !requestIsDuplicate {
			switch command.Type {
			case CommandSet:
				kv.store[command.Key] = command.Value
			case CommandDelete:
				delete(kv.store, command.Key)
			// No mutation for CommandGet
			}
			kv.lastAppliedRequest[command.ClientId] = command.RequestId
		}

		// Pipe the command to the response channel
		if ch, ok := kv.notifyChannels[applyMessage.Index]; ok {
			ch <- command
		}

		kv.mu.Unlock()

		// Make a snapshot, if necessary
		kv.makeSnapshot(applyMessage.Index)
	}
}

func (kv *KeyValueServer) makeSnapshot(lastAppliedIndex int) {
	if kv.rf.GetLogSizeSinceSnapshot() < KeyValueServerMaxLogLength {
		return
	}

	kv.mu.Lock()
	snapshotData, _ := json.Marshal(KeyValueServerSnapshot{ Store: kv.store, LastAppliedRequest: kv.lastAppliedRequest })
	kv.mu.Unlock()

	kv.rf.Snapshot(lastAppliedIndex, snapshotData)
}

func (kv *KeyValueServer) applySnapshot(applyMessage raft.ApplyMessage) {
	var snapshot KeyValueServerSnapshot
	if err := json.Unmarshal(applyMessage.Data, &snapshot); err != nil {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = snapshot.Store
	kv.lastAppliedRequest = snapshot.LastAppliedRequest
}

// Special errors for the server
var ErrNotLeader = errors.New("raft node is not leader")
var ErrNoLongerLeader = errors.New("raft node is no longer leader")
var ErrTimeout = errors.New("raft node timed out committing command")
var ErrKeyDoesNotExist = errors.New("key does not exist in store")
