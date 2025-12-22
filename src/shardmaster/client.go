package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	mu            sync.Mutex
	smLeaderId    int
	clientId      int64
	currRequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.smLeaderId = 0
	ck.clientId = nrand()
	ck.currRequestId = 0
	return ck
}

//------------------------------------------------------------------------------
// RPC calls
//------------------------------------------------------------------------------

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	ck.mu.Lock()
	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.currRequestId
	ck.currRequestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.currRequestId
	ck.currRequestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	ck.mu.Lock()
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.currRequestId
	ck.currRequestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	ck.mu.Lock()
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestId = ck.currRequestId
	ck.currRequestId++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
