package shardkv

// import "../shardmaster"
import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

//=====================================================================================================================================
// STRUCTS
//=====================================================================================================================================

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64

	Shard int // For shard migration
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	make_end      func(string) *labrpc.ClientEnd
	gid           int
	masters       []*labrpc.ClientEnd
	mck           *shardmaster.Clerk
	currentConfig shardmaster.Config

	shardKVDatabase map[int]map[string]string // shard - > shard key values
	ackedRequests   map[int64]int64           // clientId -> lastRequestId

	resultCh map[int]chan Op // log index -> chan to notify waiting RPC handler

	lastAppliedToDB int // last applied log index to the database
}

//=====================================================================================================================================
// HELPER FUNCTIONS
//=====================================================================================================================================

func (kv *ShardKV) ShardMatchesGID(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currentConfig.Shards[shard] != kv.gid {
		return false
	}
	return true
}

//=====================================================================================================================================
// GET AND PUTAPPEND HANDLER
//=====================================================================================================================================

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	requestShard := key2shard(args.Key)
	if !kv.ShardMatchesGID(requestShard) {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     requestShard,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = "Wrong Leader"
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

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	requestShard := key2shard(args.Key)
	if !kv.ShardMatchesGID(requestShard) {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     requestShard,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = "Wrong Leader"
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
func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if kv.Killed() {
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

				_, shardExists := kv.shardKVDatabase[op.Shard]
				if !shardExists {
					kv.shardKVDatabase[op.Shard] = make(map[string]string)
				}
				switch op.Operation {
				case "Put":
					kv.shardKVDatabase[op.Shard][op.Key] = op.Value
				case "Append":
					kv.shardKVDatabase[op.Shard][op.Key] += op.Value
				}
				kv.ackedRequests[op.ClientId] = op.RequestId
			}
			if op.Operation == "Get" {
				_, exists := kv.shardKVDatabase[op.Shard]
				if !exists {
					kv.shardKVDatabase[op.Shard] = make(map[string]string)
				}
				op.Value = kv.shardKVDatabase[op.Shard][op.Key]
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

func (kv *ShardKV) checkAndTakeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shardKVDatabase)
		e.Encode(kv.ackedRequests)
		e.Encode(kv.currentConfig)
		data := w.Bytes()
		kv.rf.SaveSnapshot(kv.lastAppliedToDB, data)
	}
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	// The lock is already held
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[int]map[string]string
	var ack map[int64]int64
	var config shardmaster.Config

	if d.Decode(&db) != nil || d.Decode(&ack) != nil || d.Decode(&config) != nil {
		log.Fatalf("ShardKV %d failed to read snapshot", kv.me)
	} else {
		kv.shardKVDatabase = db
		kv.ackedRequests = ack
		kv.currentConfig = config
	}
}

//=====================================================================================================================================
// CONFIG POLLER ROUTINE
//=====================================================================================================================================

func (kv *ShardKV) pollConfig() {
	for {
		if kv.Killed() {
			return
		}
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		if newConfig.Num > kv.currentConfig.Num {
			kv.currentConfig = newConfig
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//=====================================================================================================================================
// KILL AND START SERVER
//=====================================================================================================================================

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int, gid int,
	masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.make_end = make_end
	kv.gid = gid

	kv.masters = masters

	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.shardKVDatabase = make(map[int]map[string]string)
	kv.ackedRequests = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastAppliedToDB = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.readSnapshot(snapshot)
	}

	go kv.pollConfig()
	go kv.applier()

	return kv
}
