package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	Serving           = "Serving"
	Pulling           = "Pulling"
	Pushing           = "Pushing"
	GarbageCollecting = "GarbageCollecting"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type PullShardReply struct {
	Err           Err
	sharedShards  map[int]map[string]string
	ackedRequests map[int64]int64
}

type DeleteShardArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type DeleteShardReply struct {
	Err Err
}
