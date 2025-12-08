package shardmaster

import (
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	ackedRequests map[int64]int64   // clientId -> lastRequestId
	resultCh      map[int]chan Op   // log index -> chan to notify waiting RPC handler

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Operation string
	
	Servers map[int][]string // Join Args
	GIDs []int // Leave Args
	Shard int // Move Args
	GID   int // Move Args
	Num int // Query Args

	ClientId  int64
	RequestId int64
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	//Build the operatin for raft
	op := Op{
		Operation: "Join",
		Servers: args.Servers,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	//Sent to raft layer by calling start
	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Wrong Leader"
		return
	}

	sm.mu.Lock()
	ch := make(chan Op, 1)
	sm.resultCh[index] = ch
	sm.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.Err = "Wrong Leader"
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Wrong Leader"
		reply.WrongLeader = true
	}

	sm.mu.Lock()
	delete(sm.resultCh, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Operation: "Leave",
		GIDs: args.GIDs,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	//Sent to raft layer by calling start
	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Wrong Leader"
		return
	}

	sm.mu.Lock()
	ch := make(chan Op, 1)
	sm.resultCh[index] = ch
	sm.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.Err = "Wrong Leader"
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Wrong Leader"
		reply.WrongLeader = true
	}

	sm.mu.Lock()
	delete(sm.resultCh, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Operation: "Move",
		Shard: args.Shard,
		GID: args.GID,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	//Sent to raft layer by calling start
	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Wrong Leader"
		return
	}

	sm.mu.Lock()
	ch := make(chan Op, 1)
	sm.resultCh[index] = ch
	sm.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.Err = "Wrong Leader"
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Wrong Leader"
		reply.WrongLeader = true
	}

	sm.mu.Lock()
	delete(sm.resultCh, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Operation: "Query",
		Num: args.Num,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	//Sent to raft layer by calling start
	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Wrong Leader"
		return
	}

	sm.mu.Lock()
	ch := make(chan Op, 1)
	sm.resultCh[index] = ch
	sm.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.Err = "Wrong Leader"
			reply.WrongLeader = true
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "Wrong Leader"
		reply.WrongLeader = true
	}

	sm.mu.Lock()
	delete(sm.resultCh, index)
	sm.mu.Unlock()
}

//=====================================================================================================================================
// APPLIER ROUTINE
//=====================================================================================================================================

func (sm *ShardMaster) Applier() {
	
}




//=====================================================================================================================================
// KILL AND START SERVER
//=====================================================================================================================================

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})

	sm.ackedRequests = make(map[int64]int64)
	sm.resultCh = make(map[int]chan Op)

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
