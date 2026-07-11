package kv

type CommandType int
const (
	CommandSet CommandType = iota
	CommandDelete
	CommandGet
)

type Command struct {
	Type		CommandType
	Key			string
	Value		string
	// Allow identification of requests in case of nonidempotent request implementations in future
	ClientId	int
	RequestId	int
}
