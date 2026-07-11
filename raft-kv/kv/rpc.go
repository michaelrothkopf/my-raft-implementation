package kv

type RPCTransport interface {
	// Note: serverId is always the ID relative to other KeyValueServers, not to raft.Raft instances
	// Servers do not own a transport, they have no reason to; communication is not P2P but through
	//	a central router. Only Clients own a transport, which they use to communicate with the
	//	network of KeyValueServers.
	CallSet(serverId int, args *SetArgs) (*SetReply, bool)
	CallDelete(serverId int, args *DeleteArgs) (*DeleteReply, bool)
	CallGet(serverId int, args *GetArgs) (*GetReply, bool)
	// GetNextServerId gets the next sequential server ID in the network
	GetNextServerId(serverId int) int
}

type RpcReply interface {
	GetError()	error
}

type SetArgs struct {
	Key			string
	Value		string
	ClientId	int
	RequestId	int
}

type SetReply struct {
	Err			error
}

func (rp *SetReply) GetError() error {
	return rp.Err
}

type DeleteArgs struct {
	Key			string
	ClientId	int
	RequestId	int
}

type DeleteReply struct {
	Err			error
}

func (rp *DeleteReply) GetError() error {
	return rp.Err
}

type GetArgs struct {
	Key			string
	ClientId	int
	RequestId	int
}

type GetReply struct {
	Err			error
	Exists		bool
	Value		string
}

func (rp *GetReply) GetError() error {
	return rp.Err
}
