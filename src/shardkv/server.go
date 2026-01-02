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
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64

	// for reconfiguration
	Shard  int
	Config shardmaster.Config

	// for insert and garbage collection
	NewShardData  map[int]map[string]string
	AckedRequests map[int64]int64

	// for delete shard
	ShardsToDelete []int
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	make_end       func(string) *labrpc.ClientEnd
	gid            int
	masters        []*labrpc.ClientEnd
	mck            *shardmaster.Clerk // ShardMaster Clerk
	previousConfig shardmaster.Config // Just Previous config of the Updated config
	currentConfig  shardmaster.Config // Current Latest Config from ShardMaster

	shardKVDatabase map[int]map[string]string // Shard -> Shard key values
	ackedRequests   map[int64]int64           // clientId -> lastRequestId
	shardStatus     map[int]string            // Shard -> Shard Status

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

func (kv *ShardKV) isShardServing(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	status, ok := kv.shardStatus[shard]
	return ok && status == Serving
}

//=====================================================================================================================================
// GET AND PUTAPPEND RPC
//=====================================================================================================================================

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	requestShard := key2shard(args.Key)
	if !kv.ShardMatchesGID(requestShard) {
		reply.Err = ErrWrongGroup
		return
	}

	if !kv.isShardServing(requestShard) {
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

	if !kv.isShardServing(requestShard) {
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
// PULL SHARD RPC
//=====================================================================================================================================

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	reply.SharedShards = make(map[int]map[string]string)
	reply.AckedRequests = make(map[int64]int64)

	for _, shard := range args.ShardIDs {
		if kv.shardStatus[shard] == Pushing {
			// deep copy database
			reply.SharedShards[shard] = make(map[string]string)
			for k, v := range kv.shardKVDatabase[shard] {
				reply.SharedShards[shard][k] = v
			}
		}
	}

	for clientId, requestId := range kv.ackedRequests {
		reply.AckedRequests[clientId] = requestId
	}

}

//=====================================================================================================================================
// INSERT OPERATION HANDLER
//=====================================================================================================================================

func (kv *ShardKV) insertShardOperation(reply *PullShardReply) bool {
	op := Op{
		Operation:     "InsertShard",
		NewShardData:  reply.SharedShards,
		AckedRequests: reply.AckedRequests,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	success := false
	select {
	case <-ch:
		success = true
	case <-time.After(500 * time.Millisecond):
		//timeout
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()

	return success
}

func (kv *ShardKV) sendPullShardRequests(wg *sync.WaitGroup, args *PullShardArgs, serversToSend []string) {
	defer wg.Done()
	for {
		if kv.Killed() {
			return
		}

		for si := 0; si < len(serversToSend); si++ {
			srv := kv.make_end(serversToSend[si])
			var reply PullShardReply
			ok := srv.Call("ShardKV.PullShard", args, &reply)

			if ok && reply.Err == OK {
				if kv.insertShardOperation(&reply) {
					// goroutines for delete shard operaation
					go kv.sendDeleteShardRequests(args.ConfigNum, args.ShardIDs, serversToSend)
					return
				}
				return
			}

			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

//=====================================================================================================================================
// DELETE SHARD RPC
//=====================================================================================================================================

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if args.ConfigNum >= kv.currentConfig.Num {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation:      "DeleteShard",
		ShardsToDelete: args.ShardIDs,
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
	case <-ch:
		reply.Err = OK
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

//=====================================================================================================================================
// DELETE SHARD OPERATION HANDLER
//=====================================================================================================================================

func (kv *ShardKV) sendDeleteShardRequests(confignNum int, shardIds []int, serversToSend []string) {
	for {
		if kv.Killed() {
			return
		}

		for si := 0; si < len(serversToSend); si++ {
			args := DeleteShardArgs{
				ConfigNum: confignNum,
				ShardIDs:  shardIds,
			}
			srv := kv.make_end(serversToSend[si])
			var reply DeleteShardReply
			ok := srv.Call("ShardKV.DeleteShard", &args, &reply)

			if ok && reply.Err == OK {
				return
			}

			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//=====================================================================================================================================
// RECONFIGURE OPERATION HANDLER
//=====================================================================================================================================

func (kv *ShardKV) reconfigureOperation(newConfig shardmaster.Config) {

	op := Op{
		Operation: "Reconfigure",
		Config:    newConfig,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return
	}

	ch := make(chan Op, 1)
	kv.resultCh[index] = ch

	select {
	case <-ch:
		shardsToPull := make(map[int][]int) // gid -> shards to get from that gid
		kv.mu.Lock()
		for shard, status := range kv.shardStatus {
			if status == Pulling {
				previousOwnerGid := kv.previousConfig.Shards[shard]
				if previousOwnerGid == 0 {
					kv.shardStatus[shard] = Serving
				} else {
					shardsToPull[previousOwnerGid] = append(shardsToPull[previousOwnerGid], shard)
				}

			}
		}

		var wg sync.WaitGroup
		for gid, shardsArr := range shardsToPull {
			args := PullShardArgs{
				ConfigNum: kv.currentConfig.Num - 1,
				ShardIDs:  shardsArr,
			}
			wg.Add(1)
			go kv.sendPullShardRequests(&wg, &args, kv.previousConfig.Groups[gid])
		}
		kv.mu.Unlock()
		wg.Wait()

	case <-time.After(500 * time.Millisecond):
		// timeout
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()

}

//=====================================================================================================================================
// CONFIG POLLER ROUTINE
//=====================================================================================================================================

func (kv *ShardKV) pollConfig() {
	for {
		if kv.Killed() {
			return
		}
		_, isLeader := kv.rf.GetState()

		if isLeader { // only leaders can poll
			kv.mu.Lock()
			readyToUpdateConfig := true // can only update the config id all shards are serving
			for _, status := range kv.shardStatus {
				if status != Serving {
					readyToUpdateConfig = false
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num
			kv.mu.Unlock()
			if readyToUpdateConfig {
				newConfig := kv.mck.Query(currentConfigNum + 1) // for linearizability, update config by sequence
				isNewer := newConfig.Num > currentConfigNum
				if isNewer {
					kv.reconfigureOperation(newConfig)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastAppliedToDB {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			switch op.Operation {
			case "Put", "Append":
				lastRequestId, ok := kv.ackedRequests[op.ClientId]
				if !ok || op.RequestId > lastRequestId {
					if _, exists := kv.shardKVDatabase[op.Shard]; !exists {
						kv.shardKVDatabase[op.Shard] = make(map[string]string)
					}
					if op.Operation == "Put" {
						kv.shardKVDatabase[op.Shard][op.Key] = op.Value
					} else {
						kv.shardKVDatabase[op.Shard][op.Key] += op.Value
					}
					kv.ackedRequests[op.ClientId] = op.RequestId
				}

			case "Get":
				if _, exists := kv.shardKVDatabase[op.Shard]; !exists {
					kv.shardKVDatabase[op.Shard] = make(map[string]string)
				}
				op.Value = kv.shardKVDatabase[op.Shard][op.Key]

			case "Reconfigure":
				if kv.currentConfig.Num < op.Config.Num {
					kv.shardStatus = make(map[int]string)
					for shard, gid := range op.Config.Shards {
						isNewOwner := gid == kv.gid
						isOldOwner := kv.currentConfig.Shards[shard] == kv.gid

						if isNewOwner && isOldOwner {
							kv.shardStatus[shard] = Serving
						} else if isNewOwner && !isOldOwner {
							kv.shardStatus[shard] = Pulling
						} else if !isNewOwner && isOldOwner {
							kv.shardStatus[shard] = Pushing
						}
					}
					kv.previousConfig = kv.currentConfig
					kv.currentConfig = op.Config
				}

			case "InsertShard":
				for cid, rid := range op.AckedRequests {
					if currentRid, exists := kv.ackedRequests[cid]; !exists || rid > currentRid {
						kv.ackedRequests[cid] = rid
					}
				}
				for shard, kvs := range op.NewShardData {
					if kv.shardStatus[shard] == Pulling {
						kv.shardKVDatabase[shard] = make(map[string]string)
						for k, v := range kvs {
							kv.shardKVDatabase[shard][k] = v
						}
						kv.shardStatus[shard] = Serving
					}
				}

			case "DeleteShard":
				for _, shard := range op.ShardsToDelete {
					if kv.shardStatus[shard] == Pushing {
						delete(kv.shardKVDatabase, shard)
						delete(kv.shardStatus, shard)
					}
				}
			}

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
		e.Encode(kv.previousConfig)
		e.Encode(kv.shardStatus)
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
	var prevconfig shardmaster.Config
	var shardstatus map[int]string

	if d.Decode(&db) != nil || d.Decode(&ack) != nil || d.Decode(&config) != nil || d.Decode(&prevconfig) != nil || d.Decode(&shardstatus) != nil {
		log.Fatalf("ShardKV %d failed to read snapshot", kv.me)
	} else {
		kv.shardKVDatabase = db
		kv.ackedRequests = ack
		kv.currentConfig = config
		kv.previousConfig = prevconfig
		kv.shardStatus = shardstatus
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
	kv.currentConfig = kv.mck.Query(0)
	kv.previousConfig = kv.mck.Query(0)

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
