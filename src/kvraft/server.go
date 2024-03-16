package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

// debugging printer
func (kv *KVServer) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("SVER [%d] ", kv.me)
		format = prefix + format
		log.Printf(format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// operation types, OpGet, OpPut or OpAppend
	Type string

	// operation content
	// for OpGet, len(Params) == 1, Params[0] == key
	// for OpPutAppend, len(Params) == 2, Params[0] == key && Params[1] == value
	Params []string

	ClerkID   int64
	RequestID int64
}

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// K/V database
	db map[string]string

	// mapping: raftCommandIndex int -> KVServerExecutedResult chan string
	// for every clerk request to receive executed result
	requestMap sync.Map

	// request count the producer(clerk) has made
	// process count the consumer(KVServer) has made
	produceCount int
	consumeCount int

	// mapping: clerkID int64 -> KVServerExecutedResult string
	clerkPrevRequestInfo map[int64]requestInfo
}

type requestInfo struct {
	requestID int64
	result    string
}

type requestIdentifier struct {
	clerkID   int64
	requestID int64
	resultCh  chan string
}

// internal db operation
// expect the caller to hold the lock
func (kv *KVServer) get(key string) string {
	value := kv.db[key]
	// kv.DPrintf("DB GET success, key: \"%v\", value: \"%v\"", key, value)
	return value
}

// internal db operation
// expect the caller to hold the lock
func (kv *KVServer) put(key, value string) string {
	kv.db[key] = value
	// kv.DPrintf("DB PUT success, key: \"%v\", orig: \"%v\", value: \"%v\"", key, orig, value)
	return value
}

// internal db operation
// expect the caller to hold the lock
func (kv *KVServer) append(key, valToAppend string) string {
	value := kv.db[key]
	value = value + valToAppend
	kv.db[key] = value

	// kv.DPrintf("DB APPEND success, key: \"%v\", value: \"%v\"", key, value)
	return value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.DPrintf("receive GET RPC from clerk [%v], args.RequestID: \"%v\", args.Key: \"%v\"\n", args.ClerkID, args.RequestID, args.Key)

	kv.mu.RLock()
	prevRequestInfo, ok := kv.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.requestID {
		kv.mu.RUnlock()

		reply.Err = OK
		reply.Value = prevRequestInfo.result

		kv.DPrintf("requestID: \"%v\" has been executed, retrieve result from previous request, GET Key: \"%v\", Value: \"%v\"\n", args.RequestID, args.Key, prevRequestInfo.result)
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(
		Op{
			Type:      OpGet,
			Params:    []string{args.Key},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	// atomic.AddInt64(&kv.produceCount, int64(1))
	kv.mu.Lock()
	kv.produceCount += 1
	kv.mu.Unlock()

	kv.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	// TODO: use clerkID -> channel mapping since a clerk would only launch one request at the same time
	identifier := requestIdentifier{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  make(chan string),
	}
	kv.requestMap.Store(index, identifier)

	select {
	case res := <-identifier.resultCh:
		{
			kv.requestMap.Delete(index)
			reply.Err = OK
			reply.Value = res
			kv.DPrintf("raft agreement finish, GET Key: \"%v\", Value: \"%v\"\n", args.Key, res)
		}
		// TODO: consider waiting timeout
	case <-time.After(300 * time.Millisecond):
		{
			kv.requestMap.Delete(index)
			reply.Err = ErrRepTimeout
			kv.DPrintf("leader reply timeout, request fail\n")

			kv.mu.Lock()
			kv.consumeCount += 1
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.DPrintf("receive PutAppend RPC from clerk [%v], args.RequestID: \"%v\", args.Key: \"%v\", args.Value: \"%v\", args.Op: \"%v\"\n", args.ClerkID, args.RequestID, args.Key, args.Value, args.Op)

	kv.mu.RLock()
	prevRequestInfo, ok := kv.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.requestID {
		kv.mu.RUnlock()

		reply.Err = OK

		kv.DPrintf("retrieve executed result from lastRequestRes, PutAppend Key: \"%v\", Value: \"%v\"\n", args.Key, prevRequestInfo.result)
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(
		Op{
			Type:      args.Op,
			Params:    []string{args.Key, args.Value},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	// atomic.AddInt64(&kv.produceCount, int64(1))
	kv.mu.Lock()
	kv.produceCount += 1
	kv.mu.Unlock()

	kv.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	identifier := requestIdentifier{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  make(chan string),
	}
	kv.requestMap.Store(index, identifier)

	select {
	case res := <-identifier.resultCh:
		{
			kv.requestMap.Delete(index)
			reply.Err = OK
			kv.DPrintf("raft agreement finish, PutAppend Key: \"%v\", Value: \"%v\"\n", args.Key, res)
		}
	case <-time.After(300 * time.Millisecond):
		{
			kv.requestMap.Delete(index)
			reply.Err = ErrRepTimeout
			kv.DPrintf("leader reply timeout, request fail\n")

			kv.mu.Lock()
			kv.consumeCount += 1
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) handleApplyMsg() {
	for !kv.killed() {
		// TODO: handle unreliable
		applyMsg := <-kv.applyCh
		{
			kv.DPrintf("receive applyMsg, applyMsg.CommandIndex: %d\n", applyMsg.CommandIndex)

			kv.mu.Lock()
			if op, ok := applyMsg.Command.(Op); ok {
				// consider okk && prevRequestInfo.requestID >= op.RequestID
				if prevRequestInfo, okk := kv.clerkPrevRequestInfo[op.ClerkID]; !okk || op.RequestID > prevRequestInfo.requestID {
					res := kv.doApply(&applyMsg, &op)
					kv.clerkPrevRequestInfo[op.ClerkID] = requestInfo{
						requestID: op.RequestID,
						result:    res,
					}

					if requestToConsumeNum := kv.produceCount - kv.consumeCount; requestToConsumeNum > 0 {
						if entryVal, ok := kv.requestMap.Load(applyMsg.CommandIndex); ok {
							if identifier, ok := entryVal.(requestIdentifier); ok && identifier.clerkID == op.ClerkID && identifier.requestID == op.RequestID {
								kv.consumeCount += 1
								identifier.resultCh <- res
							}
						}
					} else if requestToConsumeNum == 0 {
						kv.produceCount = 0
						kv.consumeCount = 0
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) doApply(applyMsg *raft.ApplyMsg, op *Op) string {
	if applyMsg.CommandValid {
		kv.DPrintf("apply applyMsg, applyMsg.Index: %d, applyMsg.Op: %v", applyMsg.CommandIndex, op)
		if op.Type == OpGet {
			return kv.get(op.Params[0])
		} else if op.Type == OpPut {
			return kv.put(op.Params[0], op.Params[1])
		} else {
			return kv.append(op.Params[0], op.Params[1])
		}
	}

	return ""
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = map[string]string{}
	kv.requestMap = sync.Map{}
	kv.produceCount = 0
	kv.consumeCount = 0
	kv.clerkPrevRequestInfo = map[int64]requestInfo{}

	kv.DPrintf("started")

	go kv.handleApplyMsg()

	return kv
}
