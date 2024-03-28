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

	clerkID       int64
	lastRequestID int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.clerkID = nrand()
	ck.lastRequestID = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {

	ck.lastRequestID += 1

	args := GetArgs{}
	args.Key = key
	args.ClerkID = ck.clerkID
	args.RequestID = ck.lastRequestID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {

				ck.DPrintf("send GET RPC request to service, GID: %v, server: %v, Shard: %v, args.Key: %v, args.RequestID: %v\n", gid, si, shard, key, args.RequestID)

				srv := ck.make_end(servers[si])
				var reply GetReply
				if ok := srv.Call("ShardKV.Get", &args, &reply); ok {

					if reply.Err == OK {
						ck.DPrintf("receive GET response from service, args.RequestID: %v, GET Key: %v, Value: %v\n", args.RequestID, key, reply.Value)
						return reply.Value

					} else if reply.Err == ErrNoKey {
						ck.DPrintf("ErrNoKey, GET RPC fail, args.RequestID: %v, GET Key: %v\n", args.RequestID, key)
						return reply.Value

					} else if reply.Err == ErrWrongGroup {
						ck.DPrintf("ErrWrongGroup, GET RPC fail, args.RequestID: %v, GET Key: %v\n", args.RequestID, key)
						break

					}
				}
				// ... not ok, or ErrWrongLeader
			}
		}

		time.Sleep(CLERK_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.lastRequestID += 1

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {

				ck.DPrintf("send PutAppend RPC request to service, GID: %v, server: %v, Shard: %v, args.Key: %v, args.Value: %v, args.Op: %v, args.RequestID: %v\n", gid, si, shard, key, value, op, args.RequestID)

				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if ok := srv.Call("ShardKV.PutAppend", &args, &reply); ok {

					if reply.Err == OK {
						ck.DPrintf("receive PutAppend response from service, args.RequestID: %v, PutAppend Key: %v, Value: %v\n", args.RequestID, key, value)
						return

					} else if reply.Err == ErrWrongGroup {
						ck.DPrintf("ErrWrongGroup, PutAppend RPC fail, args.RequestID: %v, PutAppend Key: %v, Value: %v\n", args.RequestID, key, value)
						break

					}
				}
				// ... not ok, or ErrWrongLeader
			}
		}

		time.Sleep(CLERK_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
