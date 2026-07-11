package kv

type Client struct {
	transport		RPCTransport
	leaderHint		int	// who we believe to be the leader (a serverId in the RPCTransport)
	clientId		int
	lastRequestId	int
}

func callRpc[Args any, Reply RpcReply](cl *Client, args *Args, handler func (int, *Args) (Reply, bool)) Reply {
	for {
		reply, dead := handler(cl.leaderHint, args)
		if reply.GetError() == nil {
			return reply
		}
		if reply.GetError() == ErrNotLeader || reply.GetError() == ErrNoLongerLeader || dead {
			cl.leaderHint = cl.transport.GetNextServerId(cl.leaderHint)
			continue
		}
		if reply.GetError() == ErrTimeout {
			continue
		}
		return reply
	}
}

func (cl *Client) Set(key, value string) *SetReply {
	cl.lastRequestId++
	requestId := cl.lastRequestId
	return callRpc(cl, &SetArgs{
		Key: key,
		Value: value,
		ClientId: cl.clientId,
		RequestId: requestId,
	}, cl.transport.CallSet)
}

func (cl *Client) Delete(key string) *DeleteReply {
	cl.lastRequestId++
	requestId := cl.lastRequestId
	return callRpc(cl, &DeleteArgs{
		Key: key,
		ClientId: cl.clientId,
		RequestId: requestId,
	}, cl.transport.CallDelete)
}

func (cl *Client) Get(key string) *GetReply {
	cl.lastRequestId++
	requestId := cl.lastRequestId
	return callRpc(cl, &GetArgs{
		Key: key,
		ClientId: cl.clientId,
		RequestId: requestId,
	}, cl.transport.CallGet)
}
