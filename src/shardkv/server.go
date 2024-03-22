package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	QUERY_CONFIG_INTERVAL = 100 * time.Millisecond
	CLERK_WAITING_TIMEOUT = 200 * time.Millisecond

	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

// debugging printer
func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
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

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	mck *shardctrler.Clerk
	cfg configInfo

	// persister of this KVServer and its raft component
	persister *raft.Persister

	// K/V database
	db map[string]string

	// mapping: raftCommandIndex int -> requestResultTransmitter
	// for every clerk request to receive executed result
	requestMap sync.Map

	// request count the producer(clerk) has made
	// process count the consumer(KVServer) has made
	produceCount int
	consumeCount int

	// mapping: clerkID int64 -> KVServerExecutedResult string
	// for every clerk to check and retrieve previous executed result
	clerkPrevRequestInfo map[int64]requestInfo

	// mapping: clerkID int64 -> resultCh chan applyResult
	// for every clerk to transmit its request result
	// instead of create a resultCh for every single request
	clerkChMap map[int64]chan applyResult
}

// latest config info
// config version + my shards
type configInfo struct {
	version  int
	myShards []int
}

// id and result of last request the clerk made
// for duplicated operations detection
type requestInfo struct {
	RequestID   int64
	ApplyResult applyResult
}

// passing executed result between kv.handleApplyMsg and kv.GET/PutAppend RPC through resultCh
type requestResultTransmitter struct {
	clerkID   int64
	requestID int64

	// reference to kv.clerkChMap[clerkID]
	resultCh chan applyResult
}

// result of internal db apply result
// result value + applyOK
// TODO: handle ErrNoKey nicely
type applyResult struct {
	Result string
	OK     bool
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) get(key string) (string, bool) {
	value, ok := kv.db[key]
	return value, ok
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) put(key, value string) (string, bool) {
	kv.db[key] = value
	return value, true
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) append(key, valToAppend string) (string, bool) {
	value := kv.db[key]
	value = value + valToAppend
	kv.db[key] = value

	return value, true
}

// expect the caller to hold the lock
// check if I own this shard
func (kv *ShardKV) isMyShard(shard int) bool {
	for _, myShard := range kv.cfg.myShards {
		if myShard == shard {
			return true
		}
	}

	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	shard := key2shard(args.Key)
	kv.DPrintf("receive GET RPC from clerk [%v], args.RequestID: %v, args.Key: %v, args.Shard: %v, \n", args.ClerkID, args.RequestID, args.Key, shard)

	kv.mu.RLock()

	// check is my shard or not
	if !kv.isMyShard(shard) {
		kv.DPrintf("shard [%v] is not myShard\n", shard)
		reply.Err = ErrWrongGroup

		kv.mu.RUnlock()
		return
	}

	// check previous request info
	prevRequestInfo, ok := kv.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		kv.mu.RUnlock()

		if prevRequestInfo.ApplyResult.OK {
			reply.Err = OK
			reply.Value = prevRequestInfo.ApplyResult.Result

			kv.DPrintf("requestID: %v has been executed, retrieve result from previous request, GET Key: %v, Value: %v\n", args.RequestID, args.Key, prevRequestInfo.ApplyResult)
		} else {
			reply.Err = ErrNoKey
			kv.DPrintf("requestID: %v has been executed, GET Key: %v ErrNoKey\n", args.RequestID, args.Key)
		}

		return
	}
	kv.mu.RUnlock()

	// start a raft log consensus
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

	// prepare to receive apply result
	kv.mu.Lock()
	kv.produceCount += 1
	resultCh, ok := kv.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan applyResult)
		kv.clerkChMap[args.ClerkID] = resultCh
	}
	kv.mu.Unlock()

	kv.DPrintf("raft agreement start, applyIndex: %d\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	kv.requestMap.Store(index, receiver)

	// wait for raft consensus
	select {
	case res := <-receiver.resultCh:
		{
			kv.requestMap.Delete(index)

			if res.OK {
				reply.Err = OK
				reply.Value = res.Result
				kv.DPrintf("raft agreement finish, GET Key: %v, Value: %v\n", args.Key, res)
			} else {
				reply.Err = ErrNoKey
				kv.DPrintf("raft agreement finish, GET Key: %v, ErrNoKey", args.Key)
			}
		}
	case <-time.After(CLERK_WAITING_TIMEOUT):
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	keyShard := key2shard(args.Key)
	kv.DPrintf("receive PutAppend RPC from clerk [%v], args.RequestID: %v, args.Key: %v, args.Value: %v, args.Op: %v, args.Shard: %v\n", args.ClerkID, args.RequestID, args.Key, args.Value, args.Op, keyShard)

	kv.mu.RLock()

	if !kv.isMyShard(keyShard) {
		kv.DPrintf("shard [%v] is not myShard\n", keyShard)
		reply.Err = ErrWrongGroup

		kv.mu.RUnlock()
		return
	}

	prevRequestInfo, ok := kv.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		kv.mu.RUnlock()

		reply.Err = OK

		kv.DPrintf("requestID: %v has been executed, retrieve result from previous request, PutAppend Key: %v, Value: %v\n", args.RequestID, args.Key, prevRequestInfo.ApplyResult)
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

	kv.mu.Lock()
	kv.produceCount += 1
	resultCh, ok := kv.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan applyResult)
		kv.clerkChMap[args.ClerkID] = resultCh
	}
	kv.mu.Unlock()

	kv.DPrintf("raft agreement start, applyIndex: %d\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	kv.requestMap.Store(index, receiver)

	select {
	case res := <-receiver.resultCh:
		{
			kv.requestMap.Delete(index)

			reply.Err = OK

			kv.DPrintf("raft agreement finish, PutAppend Key: %v, Value: %v\n", args.Key, res)
		}
	case <-time.After(CLERK_REQUEST_INTERVAL):
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

// long-running listen to raft message from applyCh
func (kv *ShardKV) handleApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			// command msg
			kv.DPrintf("receive command commit applyMsg, applyMsg.CommandIndex: %d\n", applyMsg.CommandIndex)

			kv.mu.Lock()
			if op, ok := applyMsg.Command.(Op); ok {
				// check and apply command
				if prevRequestInfo, okk := kv.clerkPrevRequestInfo[op.ClerkID]; !okk || op.RequestID > prevRequestInfo.RequestID {
					res, applySuccess := kv.applyCommand(&applyMsg, &op)
					kv.clerkPrevRequestInfo[op.ClerkID] = requestInfo{
						RequestID: op.RequestID,
						ApplyResult: applyResult{
							Result: res,
							OK:     applySuccess,
						},
					}

					// check and send executed result back to request
					if numRequestToConsumeNum := kv.produceCount - kv.consumeCount; numRequestToConsumeNum > 0 {
						if entryVal, okkk := kv.requestMap.Load(applyMsg.CommandIndex); okkk {
							if sender, ok := entryVal.(requestResultTransmitter); ok && sender.clerkID == op.ClerkID && sender.requestID == op.RequestID {
								kv.consumeCount += 1
								sender.resultCh <- applyResult{
									Result: res,
									OK:     applySuccess,
								}
							}
						}
					} else if numRequestToConsumeNum == 0 {
						kv.produceCount = 0
						kv.consumeCount = 0
					}
				}

				// kv.maxraftstate = -1 for no snapshot option
				// raft state out of bound, do a snapshot
				if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
					kv.rf.Snapshot(applyMsg.SnapshotIndex, kv.generateSnapshot())
					kv.DPrintf("raft state is approaching kv.maxraftstate, snapshot created and sent to raft\n")
				}
			}
			kv.mu.Unlock()

		} else if applyMsg.SnapshotValid {
			//snapshot msg
			kv.DPrintf("reveive snapshot applyMsg, applyMsg.SnapshotTerm: %d, applyMsg.SnapshotIndex: %d\n", applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)

			kv.mu.Lock()
			kv.applySnapshot(applyMsg.Snapshot)
			kv.mu.Unlock()

			kv.DPrintf("read and apply snapshot success\n")
		}
	}
}

// expect the caller to hold the lock
// executed internal db operations
func (kv *ShardKV) applyCommand(applyMsg *raft.ApplyMsg, op *Op) (string, bool) {
	kv.DPrintf("apply command commit applyMsg, applyMsg.Index: %d, applyMsg.Op: %v", applyMsg.CommandIndex, op)

	if op.Type == OpGet {
		return kv.get(op.Params[0])
	} else if op.Type == OpPut {
		return kv.put(op.Params[0], op.Params[1])
	} else if op.Type == OpAppend {
		return kv.append(op.Params[0], op.Params[1])
	} else {
		// ignore other kinds of operation
		return "", false
	}
}

type KVSnapshot struct {
	DB                   map[string]string
	ClerkPrevRequestInfo map[int64]requestInfo
}

// expect the caller to hold the lock
// generate a snapshot of server storage: kv.db
// and detect duplicated operations state: kv.clerkPrevRequestInfo
func (kv *ShardKV) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot := KVSnapshot{
		DB:                   kv.db,
		ClerkPrevRequestInfo: kv.clerkPrevRequestInfo,
	}

	if err := e.Encode(snapshot); err != nil {
		panic(fmt.Errorf("snapshot encode fail: %w", err))
	}

	return w.Bytes()
}

// expect the caller to hold the lock
// read and apply snapshot to restore state
func (kv *ShardKV) applySnapshot(snapshotBytes []byte) {
	r := bytes.NewBuffer(snapshotBytes)
	d := labgob.NewDecoder(r)

	var snapshot KVSnapshot

	if err := d.Decode(&snapshot); err != nil {
		panic(fmt.Errorf("snapshot decode fail: %w", err))
	} else {
		// raft would pass a copy of its snap through applyCh
		// so we can reference to it safely
		kv.db = snapshot.DB
		kv.clerkPrevRequestInfo = snapshot.ClerkPrevRequestInfo
	}
}

// long-running gothread asking shardctrler for latest config every 100ms
func (kv *ShardKV) queryConfig() {
	for {
		kv.mu.Lock()

		if latestCfg := kv.mck.Query(-1); latestCfg.Num > kv.cfg.version {
			kv.DPrintf("new version config found, update to new config\n")

			kv.cfg.version = latestCfg.Num
			kv.cfg.myShards = []int{}
			for i, GID := range latestCfg.Shards {
				if GID == kv.gid {
					kv.cfg.myShards = append(kv.cfg.myShards, i)
				}
			}

			kv.DPrintf("new config: %v\n", kv.cfg)
		}

		kv.mu.Unlock()

		time.Sleep(QUERY_CONFIG_INTERVAL)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.produceCount = 0
	kv.consumeCount = 0

	kv.db = map[string]string{}
	kv.requestMap = sync.Map{}
	kv.clerkChMap = map[int64]chan applyResult{}
	kv.clerkPrevRequestInfo = map[int64]requestInfo{}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cfg = configInfo{myShards: []int{}}

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.queryConfig()
	go kv.handleApplyMsg()

	kv.DPrintf("started\n")

	return kv
}
