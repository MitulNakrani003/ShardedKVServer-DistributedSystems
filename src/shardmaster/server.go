package shardmaster

import (
	"sort"
	"sync"
	"sync/atomic"
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
	dead    int32

	ackedRequests       map[int64]int64 // clientId -> lastRequestId
	resultCh            map[int]chan Op // log index -> chan to notify waiting RPC handler
	lastAppliedToConfig int             // last applied log index to the config log

	configs       []Config // indexed by config num
	currentConfig Config
}

type Op struct {
	// Your data here.
	Operation string

	Servers map[int][]string // Join Args
	GIDs    []int            // Leave Args
	Shard   int              // Move Args
	GID     int              // Move Args
	Num     int              // Query Args

	ClientId  int64
	RequestId int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	//Build the operatin for raft
	op := Op{
		Operation: "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
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
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
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
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
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
		Num:       args.Num,
		ClientId:  args.ClientId,
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
			sm.mu.Lock()
			if args.Num == -1 || args.Num >= len(sm.configs) {
				reply.Config = sm.configs[len(sm.configs)-1]
			} else {
				reply.Config = sm.configs[args.Num]
			}
			sm.mu.Unlock()
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

func (sm *ShardMaster) applier() {
	for msg := range sm.applyCh {
		if sm.Killed() {
			return
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			sm.mu.Lock()
			if msg.CommandIndex <= sm.lastAppliedToConfig {
				sm.mu.Unlock()
				continue // Skip already applied commands
			}

			isDuplicate := false
			if op.Operation != "Query" {
				lastRequestId, ok := sm.ackedRequests[op.ClientId]
				if ok && op.RequestId <= lastRequestId {
					isDuplicate = true
				}
			}

			if !isDuplicate {
				switch op.Operation {
				case "Join":
					// Create deep copy of original
					nextConfig := sm.DeepCopyCurrentConfig()
					// Appended new joining servers from the operation args
					for key, value := range op.Servers {
						_, ok := nextConfig.Groups[key]
						if ok {
							nextConfig.Groups[key] = append(nextConfig.Groups[key], value...)
						} else {
							nextConfig.Groups[key] = value
						}
					}
					// Rebalancing
					sm.rebalanceConfigServers(&nextConfig)
					// Update Num and Append to Config Log and Update Current Config
					nextConfig.Num = sm.currentConfig.Num + 1
					sm.configs = append(sm.configs, nextConfig)
					sm.currentConfig = nextConfig
					sm.ackedRequests[op.ClientId] = op.RequestId

				case "Leave":
					// Create deep copy of original
					nextConfig := sm.DeepCopyCurrentConfig()
					// Remove the leaving groups from the operation args
					for _, gid := range op.GIDs {
						delete(nextConfig.Groups, gid)
						for shardId, assignedGid := range nextConfig.Shards {
							if assignedGid == gid {
								nextConfig.Shards[shardId] = 0
							}
						}
					}
					// Rebalancing
					sm.rebalanceConfigServers(&nextConfig)
					// Update Num and Append to Config Log and Update Current Config
					nextConfig.Num = sm.currentConfig.Num + 1
					sm.configs = append(sm.configs, nextConfig)
					sm.currentConfig = nextConfig
					sm.ackedRequests[op.ClientId] = op.RequestId

				case "Move":
					// Create deep copy of original
					nextConfig := sm.DeepCopyCurrentConfig()
					// Move the shard to the specified GID
					nextConfig.Shards[op.Shard] = op.GID
					// Update Num and Append to Config Log and Update Current Config
					nextConfig.Num = sm.currentConfig.Num + 1
					sm.configs = append(sm.configs, nextConfig)
					sm.currentConfig = nextConfig
					sm.ackedRequests[op.ClientId] = op.RequestId

				case "Query":
					// Query operation returns the current config or the specified config number
					if op.Num == -1 || op.Num >= sm.currentConfig.Num {
						op.Num = sm.currentConfig.Num
					} else {
						op.Num = sm.configs[op.Num].Num
					}

				}				
			}

			sm.lastAppliedToConfig = msg.CommandIndex
			ch, ok := sm.resultCh[msg.CommandIndex]
			sm.mu.Unlock()
			if ok {
				ch <- op
			}
		}
	}
}

//=====================================================================================================================================
// REBALANCING
//=====================================================================================================================================

// Call this function with lock held
func (sm *ShardMaster) rebalanceConfigServers(newConfig *Config) {

	numGids := len(newConfig.Groups)

	// edge case when all shards are unassigned
	if numGids == 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
		return
	}

	// sorted list of gids
	gids := make([]int, 0)
	for key, _ := range newConfig.Groups {
		gids = append(gids, key)
	}
	sort.Ints(gids)

	// calculating base and reminder
	base := int(NShards / numGids)
	remainder := NShards % numGids

	gidToTargetNumShards := make(map[int]int)
	for i, gid := range gids {
		if i < remainder {
			gidToTargetNumShards[gid] = base + 1
		} else {
			gidToTargetNumShards[gid] = base
		}
	}

	// gid -> shardnumber
	gidToShards := make(map[int][]int)
	for i, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], i)
	}
	// sort the shards
	for _, gid := range newConfig.Shards {
		sort.Ints(gidToShards[gid])
	}

	extraShardsPool := make([]int, 0)

	// all shards from GID 0 put to pool
	if shards, exists := gidToShards[0]; exists {
		extraShardsPool = append(extraShardsPool, shards...)
		delete(gidToShards, 0)
	}

	// collect shards from groups that have too many
	for _, gid := range gids {
		target := gidToTargetNumShards[gid]
		current := gidToShards[gid]
		if len(current) > target {
			excess := current[target:]
			extraShardsPool = append(extraShardsPool, excess...)
			gidToShards[gid] = current[:target] // Keep only target count
		}
	}
	sort.Ints(extraShardsPool)

	// distribute extra shards to groups that need more
	poolIdx := 0
	for _, gid := range gids {
		target := gidToTargetNumShards[gid]
		current := gidToShards[gid]
		for len(current) < target && poolIdx < len(extraShardsPool) {
			shardId := extraShardsPool[poolIdx]
			poolIdx++
			current = append(current, shardId)
		}
		gidToShards[gid] = current
	}

	// write back to the Shards array
	for _, gid := range gids {
		for _, shardId := range gidToShards[gid] {
			newConfig.Shards[shardId] = gid
		}
	}
}

//=====================================================================================================================================
// KILL AND START SERVER
//=====================================================================================================================================

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

func (sm *ShardMaster) Killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//=====================================================================================================================================
// HEALPER FUNCTIONS
//=====================================================================================================================================

func (sm *ShardMaster) DeepCopyCurrentConfig() Config {
	newConfig := Config{
		Num: sm.currentConfig.Num,
	}

	for i := 0; i < len(sm.currentConfig.Shards); i++ {
		newConfig.Shards[i] = sm.currentConfig.Shards[i]
	}

	newConfig.Groups = make(map[int][]string, len(sm.currentConfig.Groups))
	for gid, servers := range sm.currentConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	return newConfig
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.currentConfig = sm.configs[len(sm.configs)-1]

	sm.ackedRequests = make(map[int64]int64)
	sm.resultCh = make(map[int]chan Op)
	sm.lastAppliedToConfig = 0

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.applier()
	return sm
}
