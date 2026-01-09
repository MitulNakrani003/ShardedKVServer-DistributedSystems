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

	Err Err

	// for reconfiguration
	Shard  int
	Config shardmaster.Config

	// for insert and garbage collection
	ConfigNum     int
	NewShardData  map[int]map[string]string
	AckedRequests map[int64]int64
	ShardIDs      []int // for garbage collection
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

	// For garbage collection - track shards that need to be garbage collected from previous owners
	garbageShards map[int][]int // configNum -> shardIDs that need to be garbage collected
}

//=====================================================================================================================================
// HELPER FUNCTIONS
//=====================================================================================================================================

func (kv *ShardKV) isServing(shard int) bool {
	shardStatus, exists := kv.shardStatus[shard]
	if !exists || shardStatus != Serving {

		return false
	}
	return true
}

func (kv *ShardKV) ownsShard(shard int) bool {
	owns := kv.currentConfig.Shards[shard] == kv.gid
	return owns
}

func (kv *ShardKV) checkDuplicateRequest(clientId int64, requestId int64) bool {
	reqId, ok := kv.ackedRequests[clientId]
	if !ok || reqId < requestId {
		return false
	}
	return true
}

func (kv *ShardKV) areSameOperations(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Operation == b.Operation
}

//=====================================================================================================================================
// GET RPC
//=====================================================================================================================================

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	requestShard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.isServing(requestShard) || !kv.ownsShard(requestShard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	isDup := kv.checkDuplicateRequest(args.ClientId, args.RequestId)
	if isDup {
		reply.Err = OK
		reply.Value = kv.shardKVDatabase[requestShard][args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     requestShard,
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
		isSame := kv.areSameOperations(resultOp, op)
		if !isSame {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = resultOp.Err
		reply.Value = resultOp.Value
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

//=====================================================================================================================================
// PUTAPPEND RPC
//=====================================================================================================================================

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	requestShard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.isServing(requestShard) || !kv.ownsShard(requestShard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	isDup := kv.checkDuplicateRequest(args.ClientId, args.RequestId)
	if isDup {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case resultOp := <-ch:
		isSame := kv.areSameOperations(resultOp, op)
		if !isSame {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = resultOp.Err
	case <-time.After(300 * time.Millisecond):
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
	reply.ConfigNum = args.ConfigNum

	for _, shard := range args.ShardIDs {
		reply.SharedShards[shard] = make(map[string]string)
		for k, v := range kv.shardKVDatabase[shard] {
			reply.SharedShards[shard][k] = v
		}
	}

	for clientId, requestId := range kv.ackedRequests {
		reply.AckedRequests[clientId] = requestId
	}

}

//=====================================================================================================================================
// DELETE SHARD RPC - Called by new owner to tell old owner it can delete the shard data
//=====================================================================================================================================

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if args.ConfigNum >= kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation: "DeleteShardData",
		ConfigNum: args.ConfigNum,
		ShardIDs:  args.ShardIDs,
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
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.resultCh, index)
	kv.mu.Unlock()
}

//=====================================================================================================================================
// SEND PULL SHARD REQUESTS
//=====================================================================================================================================

func (kv *ShardKV) sendPullShardRequests(wg *sync.WaitGroup, args *PullShardArgs, serversToSend []string) {
	defer wg.Done()
	for {
		if kv.Killed() {
			return
		}

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			return // Let the new leader handle it
		}

		for si := 0; si < len(serversToSend); si++ {
			srv := kv.make_end(serversToSend[si])
			var reply PullShardReply
			ok := srv.Call("ShardKV.PullShard", args, &reply)

			if ok && reply.Err == OK {
				op := Op{
					Operation:     "InsertShard",
					ConfigNum:     reply.ConfigNum + 1,
					NewShardData:  reply.SharedShards,
					AckedRequests: reply.AckedRequests,
				}
				kv.rf.Start(op)
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

//=====================================================================================================================================
// RECONFIGURE OPERATION HANDLER
//=====================================================================================================================================

func (kv *ShardKV) pollShardStatusChanges() {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader { //&& !kv.allServingCurrently()
			kv.mu.Lock()

			shardsToPull := make(map[int][]int) // previousGid -> []shardIds

			for shard, status := range kv.shardStatus {
				if status == Pulling {
					previousOwnerGid := kv.previousConfig.Shards[shard]
					shardsToPull[previousOwnerGid] = append(shardsToPull[previousOwnerGid], shard)
				}
			}

			// Send pull requests to each group that has shards we need
			var wg sync.WaitGroup
			for gid, shardIds := range shardsToPull {

				servers, _ := kv.previousConfig.Groups[gid]

				args := PullShardArgs{
					ConfigNum: kv.currentConfig.Num - 1,
					ShardIDs:  shardIds,
				}

				wg.Add(1)
				go kv.sendPullShardRequests(&wg, &args, servers)
			}

			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

//=====================================================================================================================================
// CONFIG POLLER ROUTINE
//=====================================================================================================================================

// This Poller checks for new configurations from ShardMaster
// It only initiates reconfiguration if all shards are in Serving state
// Send reconfiguration operation to Raft
func (kv *ShardKV) pollConfig() {
	for !kv.Killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {

			kv.mu.Lock()
			readyToUpdateConfig := true // can only update the config id all shards are serving
			for _, status := range kv.shardStatus {
				if status != Serving {
					readyToUpdateConfig = false
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num

			if readyToUpdateConfig {

				newConfig := kv.mck.Query(currentConfigNum + 1)
				if newConfig.Num == currentConfigNum+1 {
					op := Op{
						Operation: "Reconfigure",
						Config:    newConfig,
					}
					kv.rf.Start(op)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//=====================================================================================================================================
// GARBAGE COLLECTION POLLER
//=====================================================================================================================================

func (kv *ShardKV) pollGarbageCollection() {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			garbageToProcess := make(map[int][]int)
			for configNum, shards := range kv.garbageShards {
				garbageToProcess[configNum] = shards
			}
			previousConfig := kv.previousConfig
			kv.mu.Unlock()

			var wg sync.WaitGroup
			for configNum, shardIDs := range garbageToProcess {

				cfg := kv.mck.Query(configNum)
				if cfg.Num != configNum {
					continue
				}

				shardsByGid := make(map[int][]int)
				for _, shard := range shardIDs {
					gid := cfg.Shards[shard]
					if gid != 0 {
						shardsByGid[gid] = append(shardsByGid[gid], shard)
					}
				}

				for gid, shards := range shardsByGid {
					servers, ok := previousConfig.Groups[gid]
					if !ok {
						servers, ok = cfg.Groups[gid]
					}
					if !ok || len(servers) == 0 {
						continue
					}

					wg.Add(1)
					go kv.sendDeleteShardRequests(&wg, configNum, shards, servers)
				}
			}
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) sendDeleteShardRequests(wg *sync.WaitGroup, configNum int, shardIDs []int, servers []string) {
	defer wg.Done()

	args := DeleteShardArgs{
		ConfigNum: configNum,
		ShardIDs:  shardIDs,
	}

	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply DeleteShardReply
		ok := srv.Call("ShardKV.DeleteShard", &args, &reply)

		if ok && reply.Err == OK {
			op := Op{
				Operation: "GarbageCollected",
				ConfigNum: configNum,
			}
			kv.rf.Start(op)
			return
		}
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
				if !kv.isServing(op.Shard) {
					op.Err = ErrWrongGroup
				} else {
					isDup := kv.checkDuplicateRequest(op.ClientId, op.RequestId)
					if !isDup {
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
					op.Err = OK
				}

			case "Get":
				if !kv.isServing(op.Shard) {
					op.Err = ErrWrongGroup
					op.Value = ""
				} else {
					if _, exists := kv.shardKVDatabase[op.Shard]; !exists {
						kv.shardKVDatabase[op.Shard] = make(map[string]string)
					}
					op.Value = kv.shardKVDatabase[op.Shard][op.Key]
					op.Err = OK
				}

			case "Reconfigure":
				if kv.currentConfig.Num < op.Config.Num { // checks to avoid stale reconfiguration
					newShardStatus := make(map[int]string)
					for shard, gid := range op.Config.Shards {
						if gid == kv.gid {
							// If this is the first config, or shard was previously unassigned (gid=0),
							// or we already owned this shard, mark as Serving
							if op.Config.Num == 1 || kv.currentConfig.Shards[shard] == 0 || kv.currentConfig.Shards[shard] == kv.gid {
								newShardStatus[shard] = Serving
							} else {
								// Need to pull from another group
								newShardStatus[shard] = Pulling
							}
						}
					}
					kv.shardStatus = newShardStatus
					kv.previousConfig = kv.currentConfig
					kv.currentConfig = op.Config
				}

			case "InsertShard":
				if op.ConfigNum == kv.currentConfig.Num {
					for cid, rid := range op.AckedRequests {
						if currentRid, exists := kv.ackedRequests[cid]; !exists || rid > currentRid {
							kv.ackedRequests[cid] = rid
						}
					}
					insertedShards := make([]int, 0)
					for shard, kvs := range op.NewShardData {
						if kv.shardStatus[shard] == Pulling {
							kv.shardKVDatabase[shard] = make(map[string]string)
							for k, v := range kvs {
								kv.shardKVDatabase[shard][k] = v
							}
							kv.shardStatus[shard] = Serving
							insertedShards = append(insertedShards, shard)
						}
					}
					// Track shards that need garbage collection at the source
					if len(insertedShards) > 0 {
						if _, exists := kv.garbageShards[op.ConfigNum-1]; !exists {
							kv.garbageShards[op.ConfigNum-1] = make([]int, 0)
						}
						kv.garbageShards[op.ConfigNum-1] = append(kv.garbageShards[op.ConfigNum-1], insertedShards...)
					}
				}

			case "DeleteShardData":
				// The old owner deletes shard data after new owner confirms receipt
				// Only delete if we no longer own these shards
				if op.ConfigNum < kv.currentConfig.Num {
					for _, shard := range op.ShardIDs {
						if kv.currentConfig.Shards[shard] != kv.gid {
							delete(kv.shardKVDatabase, shard)
						}
					}
				}

			case "GarbageCollected":
				delete(kv.garbageShards, op.ConfigNum)
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
		e.Encode(kv.shardStatus)
		e.Encode(kv.garbageShards)
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
	var shardstatus map[int]string
	var garbageShards map[int][]int

	if d.Decode(&db) != nil || d.Decode(&ack) != nil || d.Decode(&config) != nil || d.Decode(&shardstatus) != nil || d.Decode(&garbageShards) != nil {
		log.Fatalf("ShardKV %d failed to read snapshot", kv.me)
	} else {
		kv.shardKVDatabase = db
		kv.ackedRequests = ack
		kv.currentConfig = config
		kv.shardStatus = shardstatus
		if garbageShards != nil {
			kv.garbageShards = garbageShards
		} else {
			kv.garbageShards = make(map[int][]int)
		}
		kv.previousConfig = kv.mck.Query(kv.currentConfig.Num - 1)
	}
}

//=====================================================================================================================================
// KILL AND START SERVER
//=====================================================================================================================================

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.shardStatus = make(map[int]string)
	kv.garbageShards = make(map[int][]int)

	kv.resultCh = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastAppliedToDB = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.readSnapshot(snapshot)
	}

	go kv.pollConfig()
	go kv.pollShardStatusChanges()
	go kv.pollGarbageCollection()
	go kv.applier()

	return kv
}
