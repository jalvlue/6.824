package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	// clerk request interval in the face of wrong leader or bad network
	// REQUEST_INTERVAL = 50 * time.Millisecond
	REQUEST_INTERVAL = 100 * time.Millisecond
)

// debugging printer
func (ck *Clerk) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("CLRK [%v] ", ck.clerkID)
		format = prefix + format
		log.Printf(format, a...)
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	clerkID       int64
	lastRequestID int64
	lastLeaderID  int
	numServers    int
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
	// Your code here.

	ck.clerkID = nrand()
	ck.lastRequestID = 0
	ck.numServers = len(servers)
	ck.lastLeaderID = 0

	ck.DPrintf("clerk start\n")
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// Your code here.

	ck.lastRequestID += 1

	reply := &QueryReply{}
	args := &QueryArgs{
		Num:       num,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for {
		// ck.DPrintf("send Query RPC request to service, args.Num: \"%v\", args.RequestID: \"%v\"\n", num, args.RequestID)
		if ok := ck.servers[leader].Call("ShardCtrler.Query", args, reply); ok && reply.Err == OK {
			// ck.DPrintf("receive Query response form service, reply.Config: %v\n", reply.Config)

			ck.lastLeaderID = leader
			return reply.Config
		}

		reply.Err = ErrDefault
		leader = (leader + 1) % ck.numServers
		time.Sleep(REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	// Your code here.

	ck.lastRequestID += 1

	reply := &JoinReply{}
	args := &JoinArgs{
		Servers:   servers,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for {
		// ck.DPrintf("send Join RPC request to service, args.RequestID: \"%v\"\n", args.RequestID)
		if ok := ck.servers[leader].Call("ShardCtrler.Join", args, reply); ok && reply.Err == OK {
			// ck.DPrintf("receive Join response form service\n")

			ck.lastLeaderID = leader
			return
		}

		reply.Err = ErrDefault
		leader = (leader + 1) % ck.numServers
		time.Sleep(REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Leave(gids []int) {

	// Your code here.

	ck.lastRequestID += 1

	reply := &LeaveReply{}
	args := &LeaveArgs{
		GIDs:      gids,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for {
		// ck.DPrintf("send Leave RPC request to service, args.GIDs: %v, args.RequestID: \"%v\"\n", gids, args.RequestID)
		if ok := ck.servers[leader].Call("ShardCtrler.Leave", args, reply); ok && reply.Err == OK {
			// ck.DPrintf("receive Leave response form service\n")

			ck.lastLeaderID = leader
			return
		}

		reply.Err = ErrDefault
		leader = (leader + 1) % ck.numServers
		time.Sleep(REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	// Your code here.

	ck.lastRequestID += 1

	reply := &MoveReply{}
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for {
		// ck.DPrintf("send Move RPC request to service, args.Shard: \"%v\", args.GID: \"%v\", args.RequestID: \"%v\"\n", shard, gid, args.RequestID)
		if ok := ck.servers[leader].Call("ShardCtrler.Move", args, reply); ok && reply.Err == OK {
			// ck.DPrintf("receive Move response form service\n")

			ck.lastLeaderID = leader
			return
		}

		reply.Err = ErrDefault
		leader = (leader + 1) % ck.numServers
		time.Sleep(REQUEST_INTERVAL)
	}
}
