package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

const (
	// clerk request interval in the face of wrong leader
	CLERK_REQUEST_INTERVAL = 100 * time.Millisecond
)

func (ck *Clerk) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("CLRK [%v] ", ck.clerkID)
		format = prefix + format
		log.Printf(format, a...)
	}
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	clerkID   int64
	requestID int64

	// mapping: gid -> leaderServerID
	leaderIDs map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		clerkID:   nrand(),
		requestID: 0,
		leaderIDs: map[int]int{},
	}

	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) command(args *CommandArgs) string {
	args.ClerkID = ck.clerkID
	args.RequestID = ck.requestID

	for {
		keyShardID := key2shard(args.Key)
		gid := ck.config.Shards[keyShardID]
		leaderID := ck.leaderIDs[gid]

		if servers, ok := ck.config.Groups[gid]; ok {

			for {
				ck.DPrintf("send command request to service, GID: %v, server: %v, Shard: %v, args.RequestID: %v, args.CommandOp: %v, args.Key: %v, args.Value: %v\n", gid, leaderID, keyShardID, args.RequestID, args.CommandOp, args.Key, args.Value)

				var reply CommandReply
				if ok := ck.make_end(servers[leaderID]).Call("ShardKV.Command", args, &reply); ok {

					if reply.Err == OK || reply.Err == ErrNoKey {
						ck.DPrintf("receive command response from service, reply.Err: %v, reply.Value: %v, args.RequestID: %v, args.CommandOp: %v, args.Key: %v, args.Value: %v\n", args.RequestID, reply.Err, reply.Value, args.CommandOp, args.Key, args.Value)

						ck.requestID += 1
						ck.leaderIDs[gid] = leaderID
						return reply.Value
					} else if reply.Err == ErrWrongGroup {
						ck.DPrintf("ErrWrongGroup, command request fail, args.RequestID: %v, args.Op: %v, args.Key: %v, args.Value: %v\n", args.RequestID, args.CommandOp, args.Key, args.Value)
						break
					}
				}
				// ... not ok, or ErrWrongLeader
				// try next server
				leaderID = (leaderID + 1) % len(servers)
			}
		}

		time.Sleep(CLERK_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.command(&CommandArgs{
		Key:       key,
		CommandOp: CommandGet,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.command(&CommandArgs{
		Key:       key,
		Value:     value,
		CommandOp: CommandPut,
	})
}

func (ck *Clerk) Append(key string, value string) {
	ck.command(&CommandArgs{
		Key:       key,
		Value:     value,
		CommandOp: CommandAppend,
	})
}
