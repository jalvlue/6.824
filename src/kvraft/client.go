package kvraft

import (
	"crypto/rand"
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
		format = "CLRK " + format
		log.Printf(format, a...)
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	numServers int

	// ID of leader that clerk communicate successfully last time
	lastLeaderID int64
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

	ck.DPrintf("CLRK start\n")
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

	reply := &GetReply{}
	args := &GetArgs{key}

	leader := ck.lastLeaderID
	for reply.Err != ErrNoKey {
		ck.DPrintf("send GET RPC request to service, args.Key: \"%v\"\n", key)
		if ok := ck.servers[leader].Call("KVServer.Get", args, reply); ok {
			if reply.Err == OK {
				ck.DPrintf("receive GET RPC response from service, Key: \"%v\", Value: \"%v\"\n", key, reply.Value)

				ck.lastLeaderID = leader
				return reply.Value
			}
		}

		reply.Err = ErrDefault
		time.Sleep(REQUEST_INTERVAL)

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

	reply := &PutAppendReply{}
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	leader := ck.lastLeaderID
	// for err == ErrWrongLeader {
	for {
		ck.DPrintf("send PutAppend RPC request to service, args.Key: \"%v\", args.Value: \"%v\", args.Op: \"%v\"\n", key, value, op)

		if ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply); ok {
			if reply.Err == OK {
				ck.DPrintf("receive PutAppend RPC response from srevice, PutAppend key: \"%v\", value: \"%v\" success\n", key, value)

				ck.lastLeaderID = leader
				return
			}
		}

		reply.Err = ErrDefault
		time.Sleep(REQUEST_INTERVAL)

		leader = ck.chooseRandomServerToSendRPC()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
