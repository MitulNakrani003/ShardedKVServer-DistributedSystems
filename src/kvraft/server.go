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

//=====================================================================================================================================
// STRUCTS
//=====================================================================================================================================

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

	lastAppliedToDB int // last applied log index to the database
}

type Op struct {
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

//=====================================================================================================================================
// GET AND PUTAPPEND HANDLER
//=====================================================================================================================================

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
			reply.Err = OK
			reply.Value = resultOp.Value
		} else {
			reply.Err = ErrWrongLeader
		}
		DPrintf("Check Returned Response: resultOp.ClientId=%v, resultOp.RequestId=%v, op.ClientId=%v, op.RequestId=%v", resultOp.ClientId, resultOp.RequestId, op.ClientId, op.RequestId)

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
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

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

//=====================================================================================================================================
// APPLIER ROUTINE
//=====================================================================================================================================

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
			if msg.CommandIndex <= kv.lastAppliedToDB {
				kv.mu.Unlock()
				continue // Skip already applied commands
			}

			// Duplicate detection
			lastRequestId, ok := kv.ackedRequests[op.ClientId]
			if !ok || op.RequestId > lastRequestId { // If this is a new request or a higher request ID
				switch op.Operation {
				case "Put": 
					kv.database[op.Key] = op.Value
				case "Append":
					kv.database[op.Key] += op.Value
				}
				kv.ackedRequests[op.ClientId] = op.RequestId
			}
			if op.Operation == "Get" {
				op.Value = kv.database[op.Key]
			}

			// Update last applied index
			kv.lastAppliedToDB = msg.CommandIndex

			ch, ok := kv.resultCh[msg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				ch <- op
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.ConditionToInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readSnapshot(msg.Snapshot)
				kv.lastAppliedToDB = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
		kv.checkAndTakeSnapshot()
	}
}

func (kv *KVServer) checkAndTakeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.database)
		e.Encode(kv.ackedRequests)
		data := w.Bytes()
		kv.rf.SaveSnapshot(kv.lastAppliedToDB, data)
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	// The lock is already held
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var ack map[int64]int64
	if d.Decode(&db) != nil || d.Decode(&ack) != nil {
		log.Fatalf("KVServer %d failed to read snapshot", kv.me)
	} else {
		kv.database = db
		kv.ackedRequests = ack
	}
}

//=====================================================================================================================================
// KILL AND START SERVER
//=====================================================================================================================================

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
	kv.lastAppliedToDB = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 { // bootstrap from snapshot check if any exists
		kv.readSnapshot(snapshot)
	}

	go kv.applier()
	return kv
}
