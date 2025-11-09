package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	database      map[string]string // key-value store
	ackedRequests map[int64]int64   // clientId -> lastRequestId
	resultCh      map[int]chan Op   // log index -> chan to notify waiting RPC handler
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	// Start the operation in Raft
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			kv.mu.Lock()
			value, ok := kv.database[op.Key]
			kv.mu.Unlock()
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongLeader
		}
		DPrintf("Check Returned Response: resultOp.ClientId=%v, resultOp.RequestId=%v, op.ClientId=%v, op.RequestId=%v", resultOp.ClientId, resultOp.RequestId, op.ClientId, op.RequestId)
		// Clean up the channel after successful completion
		kv.mu.Lock()
		delete(kv.resultCh, index)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		// Clean up the channel on timeout to prevent deadlock
		kv.mu.Lock()
		delete(kv.resultCh, index)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case resultOp := <-ch:
		if resultOp.ClientId == op.ClientId && resultOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		DPrintf("Check Returned Response: resultOp.ClientId=%v, resultOp.RequestId=%v, op.ClientId=%v, op.RequestId=%v", resultOp.ClientId, resultOp.RequestId, op.ClientId, op.RequestId)
		// Clean up the channel on timeout to prevent deadlock
		// Very important to prevent memory leak
		// Stuck Here for 3 Hours to solve deadlock problem
		kv.mu.Lock()
		delete(kv.resultCh, index)
		kv.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		// Clean up the channel on timeout to prevent deadlock
		// Very important to prevent memory leak
		kv.mu.Lock()
		delete(kv.resultCh, index)
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// It blindly executes the operations in the exact sequence it receives them from Raft.
// It also handles duplicate detection
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.CommandValid {
			op := msg.Command.(Op) // Cast the command to an Op struct
			kv.mu.Lock()

			lastRequestId, ok := kv.ackedRequests[op.ClientId]
			if !ok || op.RequestId > lastRequestId { // If this is a new request or a higher request ID
				switch op.Operation { // Only PUT and APPEND here to make seperation in concerns for database access
				case "Put": // Additional GET condition here can create a bottleneck of the applier function just to read DB and can make raft slow 
					kv.database[op.Key] = op.Value
				case "Append":
					kv.database[op.Key] += op.Value
				}
				kv.ackedRequests[op.ClientId] = op.RequestId
			}

			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.takeSnapshot(msg.CommandIndex)
			}

			ch, ok := kv.resultCh[msg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				ch <- op
			}
		}
	}
}

func (kv *KVServer) takeSnapshot(lastAppliedIndextoDB int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.ackedRequests)
	data := w.Bytes()
	kv.rf.SaveSnapshot(lastAppliedIndextoDB, data)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.database = make(map[string]string)
	kv.ackedRequests = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	return kv
}
