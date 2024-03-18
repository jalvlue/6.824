package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	// clerk request interval in the face of wrong leader
	REQUEST_INTERVAL = 50 * time.Millisecond
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
	// You will have to modify this struct.

	numServers int

	// ID of leader that clerk communicate successfully last time
	lastLeaderID int64

	// randomly generated ID for service to remember clerk
	clerkID int64

	// ID of last request
	lastRequestID int64
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
	// You'll have to add code here.

	ck.numServers = len(servers)
	ck.lastLeaderID = ck.chooseRandomServerToSendRPC()
	ck.clerkID = nrand()
	ck.lastRequestID = 0

	ck.DPrintf("clerk start\n")
	return ck
}

// generate a random server ID
func (ck *Clerk) chooseRandomServerToSendRPC() int64 {
	return nrand() % int64(ck.numServers)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// TODO: maybe handle clerk agent for multiple concurrent clients
	ck.lastRequestID += 1

	reply := &GetReply{}
	args := &GetArgs{
		Key:       key,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for reply.Err != ErrNoKey {
		ck.DPrintf("send GET RPC request to service, args.Key: \"%v\", args.RequestID: \"%v\"\n", key, args.RequestID)
		if ok := ck.servers[leader].Call("KVServer.Get", args, reply); ok {
			if reply.Err == OK {
				ck.DPrintf("receive GET RPC response from service, args.RequestID: \"%v\", Key: \"%v\", Value: \"%v\"\n", args.RequestID, key, reply.Value)

				ck.lastLeaderID = leader
				return reply.Value
			}

			reply.Err = ErrDefault
			time.Sleep(REQUEST_INTERVAL)
		} else {
			// leader crash or RPC message drop
			ck.DPrintf("leader crashes or RPC message drop, do quickly resent\n")
		}

		leader = ck.chooseRandomServerToSendRPC()
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.lastRequestID += 1

	reply := &PutAppendReply{}
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkID:   ck.clerkID,
		RequestID: ck.lastRequestID,
	}

	leader := ck.lastLeaderID
	for {
		ck.DPrintf("send PutAppend RPC request to service, args.Key: \"%v\", args.Value: \"%v\", args.Op: \"%v\", args.RequestID: \"%v\"\n", key, value, op, args.RequestID)

		if ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply); ok {
			if reply.Err == OK {
				ck.DPrintf("receive PutAppend RPC response from srevice, PutAppend Key: \"%v\", Value: \"%v\", args.RequestID: \"%v\" success\n", key, value, args.RequestID)

				ck.lastLeaderID = leader
				return
			}

			reply.Err = ErrDefault
			time.Sleep(REQUEST_INTERVAL)
		} else {
			ck.DPrintf("leader crashes or RPC message drop, do quickly resent\n")
		}

		leader = ck.chooseRandomServerToSendRPC()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
