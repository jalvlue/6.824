package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	// QUERY_CONFIG_INTERVAL = 50 * time.Millisecond
	QUERY_CONFIG_INTERVAL = 100 * time.Millisecond
	CLERK_WAITING_TIMEOUT = 200 * time.Millisecond

	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
	OpInit   = "Init"
)

// debugging printer
func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[GID: %d, SVER: %d] ", kv.gid, kv.me)
		format = prefix + format
		log.Printf(format, a...)
	}
}

// transfer config command
// sync new config to peers through raft consensus
type ConfigTransmitter struct {
	GlobalConfig shardctrler.Config
	InShards     map[int]struct{}
	LeaveShards  map[int]struct{}

	// ShardDB              []map[string]string
	// ShardPrevRequestInfo []map[int64]requestInfo
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
	dead         int32
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
	// mapping: shardID -> ShardDB
	db map[int]*ShardDB

	// mapping: raftCommandIndex int -> requestResultTransmitter
	// for every clerk request to receive executed result
	requestMap sync.Map

	// request count the producer(clerk) has made
	// process count the consumer(KVServer) has made
	produceCount int
	consumeCount int

	// mapping: clerkID int64 -> resultCh chan applyResult
	// for every clerk to transmit its request result
	// instead of create a resultCh for every single request
	clerkChMap map[int64]chan applyResult
}

// lateset config info as far as I know
// globalConfig + myShards(shards I own)
type configInfo struct {
	GlobalConfig shardctrler.Config
	MyShards     map[int]struct{}
}

// every shard is consider to be a individual database
// for the sake of shard migration
type ShardDB struct {
	// internal KVMap
	// mapping: key -> value
	KVMap map[string]string

	// for duplicated operations detection
	// mapping: clerkID int64 -> KVServerExecutedResult
	DupMap map[int64]requestInfo

	// Valid bool
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
// result value + applyErr
type applyResult struct {
	Result string
	Err    Err
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) get(key string) (string, Err) {
	keyShard := key2shard(key)
	if value, ok := kv.db[keyShard].KVMap[key]; ok {
		return value, OK
	}

	return "", ErrNoKey
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) put(key, value string) (string, Err) {
	keyShard := key2shard(key)
	kv.db[keyShard].KVMap[key] = value
	return value, OK
}

// internal db operation
// expect the caller to hold the lock
func (kv *ShardKV) append(key, valToAppend string) (string, Err) {
	keyShard := key2shard(key)
	value := kv.db[keyShard].KVMap[key]
	value = value + valToAppend
	kv.db[keyShard].KVMap[key] = value

	return value, OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		// kv.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	keyShard := key2shard(args.Key)
	kv.DPrintf("receive GET RPC from clerk [%v], args.RequestID: %v, args.Key: %v, args.Shard: %v, \n", args.ClerkID, args.RequestID, args.Key, keyShard)

	kv.mu.RLock()

	// check is my shard or not
	if !isMyShard(kv.cfg.MyShards, keyShard) {
		kv.mu.RUnlock()

		kv.DPrintf("shard [%v] is not myShard\n", keyShard)
		reply.Err = ErrWrongGroup
		return
	}

	// check previous request info
	prevRequestInfo, ok := kv.db[keyShard].DupMap[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		kv.mu.RUnlock()

		reply.Err = prevRequestInfo.ApplyResult.Err
		reply.Value = prevRequestInfo.ApplyResult.Result

		kv.DPrintf("requestID: %v has been executed, retrieve result from previous request, GET Key: %v, Value: %v\n", args.RequestID, args.Key, prevRequestInfo.ApplyResult.Result)

		return
	}
	kv.mu.RUnlock()

	// start a raft log consensus
	index, _, _ := kv.rf.Start(
		Op{
			Type:      OpGet,
			Params:    []string{args.Key},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

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

			reply.Err = res.Err
			reply.Value = res.Result
			kv.DPrintf("raft agreement finish, GET Key: %v, Value: %v, Err: %v\n", args.Key, res.Result, res.Err)
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

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		// kv.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	keyShard := key2shard(args.Key)
	kv.DPrintf("receive PutAppend RPC from clerk [%v], args.RequestID: %v, args.Key: %v, args.Value: %v, args.Op: %v, args.Shard: %v\n", args.ClerkID, args.RequestID, args.Key, args.Value, args.Op, keyShard)

	kv.mu.RLock()

	if !isMyShard(kv.cfg.MyShards, keyShard) {
		kv.mu.RUnlock()

		kv.DPrintf("shard [%v] is not myShard\n", keyShard)
		reply.Err = ErrWrongGroup
		return
	}

	if prevRequestInfo, ok := kv.db[keyShard].DupMap[args.ClerkID]; ok && args.RequestID == prevRequestInfo.RequestID {
		kv.mu.RUnlock()

		reply.Err = OK

		kv.DPrintf("requestID: %v has been executed, retrieve result from previous request, PutAppend Key: %v, Value: %v\n", args.RequestID, args.Key, prevRequestInfo.ApplyResult)
		return
	}
	kv.mu.RUnlock()

	index, _, _ := kv.rf.Start(
		Op{
			Type:      args.Op,
			Params:    []string{args.Key, args.Value},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

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

			reply.Err = res.Err
			kv.DPrintf("raft agreement finish, PutAppend Key: %v, Value: %v, Err: %v\n", args.Key, res.Result, res.Err)
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
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			// command msg

			kv.mu.Lock()

			// apply config change
			if cfgTransmitter, ok := applyMsg.Command.(ConfigTransmitter); ok {

				kv.DPrintf("receive config change applyMsg, applyMsg.CommandIndex: %d, new config: %v, inShards: %v, leaveShards: %v\n", applyMsg.CommandIndex, cfgTransmitter.GlobalConfig, cfgTransmitter.InShards, cfgTransmitter.LeaveShards)

				kv.cfg.GlobalConfig = cfgTransmitter.GlobalConfig
				for inShardID := range cfgTransmitter.InShards {
					kv.cfg.MyShards[inShardID] = struct{}{}
					kv.db[inShardID] = &ShardDB{
						KVMap:  map[string]string{},
						DupMap: map[int64]requestInfo{},
					}
				}

				for leaveShardID := range cfgTransmitter.LeaveShards {
					delete(kv.db, leaveShardID)
				}

				kv.DPrintf("config change apply, kv.cfg.Myshards: %v\n", kv.cfg.MyShards)

				// // it is safe to iter a nil slice
				// for i, shard := range cfgTransmitter.InShards {
				// 	kv.db[shard] = cfgTransmitter.ShardDB[i]
				// 	kv.clerkPrevRequestInfo[shard] = cfgTransmitter.ShardPrevRequestInfo[i]
				// }

				// kv.rf.Snapshot(applyMsg.CommandIndex, kv.generateSnapshot())
				// kv.DPrintf("new config apply, snapshot created and sent to raft\n")

				kv.mu.Unlock()
				continue
			}

			// apply DB operation
			if op, ok := applyMsg.Command.(Op); ok {

				if op.Type == OpInit {
					kv.mu.Unlock()
					kv.DPrintf("receive initial commit applyMsg, applyMsg.CommandIndex: %v\n", applyMsg.CommandIndex)
					continue
				}

				kv.DPrintf("receive DB operation applyMsg, applyMsg.CommandIndex: %d\n", applyMsg.CommandIndex)

				// config change happen, this shard is no longer mine, do not execute
				if keyShard := key2shard(op.Params[0]); !isMyShard(kv.cfg.MyShards, keyShard) {

					// return ErrWrongGroup to waiting clerk (if any)
					if entryVal, ok := kv.requestMap.Load(applyMsg.CommandIndex); ok {
						if sender, ok := entryVal.(requestResultTransmitter); ok && sender.clerkID == op.ClerkID && sender.requestID == op.RequestID {
							kv.consumeCount += 1
							sender.resultCh <- applyResult{
								Err: ErrWrongGroup,
							}
						}
					}
				} else {

					// check and apply command
					if prevRequestInfo, okk := kv.db[keyShard].DupMap[op.ClerkID]; !okk || op.RequestID > prevRequestInfo.RequestID {
						res, applySuccess := kv.applyCommand(&applyMsg, &op)
						kv.db[keyShard].DupMap[op.ClerkID] = requestInfo{
							RequestID: op.RequestID,
							ApplyResult: applyResult{
								Result: res,
								Err:    applySuccess,
							},
						}

						// check and send executed result back to request
						if numRequestToConsumeNum := kv.produceCount - kv.consumeCount; numRequestToConsumeNum > 0 {
							if entryVal, okkk := kv.requestMap.Load(applyMsg.CommandIndex); okkk {
								if sender, ok := entryVal.(requestResultTransmitter); ok && sender.clerkID == op.ClerkID && sender.requestID == op.RequestID {
									kv.consumeCount += 1
									sender.resultCh <- applyResult{
										Result: res,
										Err:    applySuccess,
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
						kv.rf.Snapshot(applyMsg.CommandIndex, kv.generateSnapshot())
						kv.DPrintf("raft state is approaching kv.maxraftstate, snapshot created and sent to raft\n")
					}
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
func (kv *ShardKV) applyCommand(applyMsg *raft.ApplyMsg, op *Op) (string, Err) {
	kv.DPrintf("apply command commit applyMsg, applyMsg.Index: %d, applyMsg.Op: %v", applyMsg.CommandIndex, op)

	if op.Type == OpGet {
		return kv.get(op.Params[0])
	} else if op.Type == OpPut {
		return kv.put(op.Params[0], op.Params[1])
	} else if op.Type == OpAppend {
		return kv.append(op.Params[0], op.Params[1])
	} else {
		// ignore other kinds of operation
		return "", ErrDefault
	}
}

type KVSnapshot struct {
	DB  map[int]*ShardDB
	Cfg configInfo
}

// expect the caller to hold the lock
// generate a snapshot of server storage: kv.db
// and detect duplicated operations state: kv.clerkPrevRequestInfo
func (kv *ShardKV) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot := KVSnapshot{
		DB:  kv.db,
		Cfg: kv.cfg,
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
		kv.cfg = snapshot.Cfg
	}
}

// func (kv *ShardKV) PullShard(args *TransferShardArgs, reply *TransferShardReply) {
// 	if _, isLeader := kv.rf.GetState(); !isLeader {
// 		reply.Err = ErrWrongLeader
// 		kv.DPrintf("receive PullShard RPC, I am not Leader, return with ErrWrongLeader\n")
// 		return
// 	}

// 	kv.DPrintf("receive PullShard RPC form GID %v, args.Shard, args.Version\n", args.GID, args.Shard, args.Num)

// 	kv.mu.RLock()
// 	// different num
// 	if args.Num != kv.cfg.Num {
// 		if args.Num > kv.cfg.Num {
// 			reply.Err = ErrHigherNum
// 		} else {
// 			reply.Err = ErrLowerNum
// 		}

// 		kv.DPrintf("ErrWrongNum, args.Num: %v, kv.cfg.Num: %v\n", args.Num, kv.cfg.Num)

// 		kv.mu.RUnlock()
// 		return
// 	}

// 	reply.Err = OK
// 	reply.ShardDB = kv.db[args.Shard]
// 	reply.ShardPrevRequestInfo = kv.clerkPrevRequestInfo[args.Shard]

// 	kv.DPrintf("shard: %v transfer to GID: %v success\n", args.Shard, args.GID)

// 	kv.mu.RUnlock()
// }

// // pull inShards from other groups
// func (kv *ShardKV) pullInShards(inShards []int, cfgTransmitter *ConfigTransmitter) {

// 	var wg sync.WaitGroup
// 	wg.Add(len(inShards))

// 	for i, shard := range inShards {

// 		go func(i, shard int, wg *sync.WaitGroup) {
// 			gid := lastCfg.Shards[shard]
// 			if servers, ok := lastCfg.Groups[gid]; ok {
// 				args := TransferShardArgs{
// 					GID:   kv.gid,
// 					Shard: shard,
// 					Num:   kv.cfg.Num,
// 				}

// 				kv.DPrintf("for inShard: %v, GID: %v\n", shard, gid)

// 				// for !kv.killed() {
// 				for si := 0; si < len(servers); si++ {
// 					srv := kv.make_end(servers[si])
// 					var reply TransferShardReply

// 					kv.DPrintf("send PullShard RPC request to GID: %v, serverID: %v, args,Shard: %v, args.Num: %v\n", gid, si, shard, shard, args.Num)

// 					if ok := srv.Call("ShardKV.PullShard", &args, &reply); ok {

// 						if reply.Err == OK {
// 							cfgTransmitter.ShardDB[i] = reply.ShardDB
// 							cfgTransmitter.ShardPrevRequestInfo[i] = reply.ShardPrevRequestInfo

// 							kv.DPrintf("receive PullShard RPC response from GID: %v, serverID: %v, request success, shard: %v pulled\n", gid, si, shard)

// 							wg.Done()
// 							return

// 						} else if reply.Err == ErrLowerNum {

// 							kv.DPrintf("receive PullShard RPC response from GID: %v, serverID: %v, request failed, ErrLowerNum\n", gid, si)

// 							wg.Done()
// 							return
// 						}
// 					}
// 				}
// 				// }
// 			}
// 		}(i, shard, &wg)
// 	}

// 	wg.Wait()
// }

// long-running go routine asking shardctrler for latest config every 100ms
func (kv *ShardKV) queryConfig() {
	initialCommit := true
	for !kv.killed() {

		// only the leader of this group would poll the latest config
		// other peers would get the latest config through raft consensus
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// kv.DPrintf("I am not leader\n")
			time.Sleep(QUERY_CONFIG_INTERVAL)
			continue
		}

		// initial commit to bring all states back after a crash (if any)
		if initialCommit {
			initialCommit = false
			kv.rf.Start(Op{
				Type: OpInit,
			})
			time.Sleep(QUERY_CONFIG_INTERVAL)
			continue
		}

		kv.mu.RLock()

		if newCfg := kv.mck.Query(kv.cfg.GlobalConfig.Num + 1); newCfg.Num > kv.cfg.GlobalConfig.Num {
			kv.DPrintf("new config found, prepare updating to version: %v\n", newCfg.Num)

			newMyShards := map[int]struct{}{}
			for shardID := range newCfg.Shards {
				if kv.gid == newCfg.Shards[shardID] {
					newMyShards[shardID] = struct{}{}
				}
			}

			inShards, leaveShards := getInAndLeaveShards(kv.cfg.MyShards, newMyShards)

			cfgTransmitter := ConfigTransmitter{
				GlobalConfig: newCfg,
				InShards:     inShards,
				LeaveShards:  leaveShards,
			}

			index, _, _ := kv.rf.Start(
				cfgTransmitter,
			)

			kv.DPrintf("begin syncing new config with peers through raft, applyIndex: %v\n", index)
		}

		// // only update one version in a query
		// if newCfg := kv.mck.Query(kv.cfg.Num + 1); newCfg.Num > kv.cfg.Num {
		// 	kv.DPrintf("new config found, update to version: %v\n", newCfg.Num)
		// 	newMyShards := map[int]struct{}{}
		// 	for shard, GID := range newCfg.Shards {
		// 		if GID == kv.gid {
		// 			newMyShards[shard] = struct{}{}
		// 		}
		// 	}
		// 	inShards, leaveShards := getInAndLeaveShards(kv.cfg.MyShards, newMyShards)
		// 	kv.DPrintf("oldMyShards: %v, newMyShards: %v, inShards: %v, leaveShards: %v\n", kv.cfg.MyShards, newMyShards, inShards, leaveShards)
		// 	cfgTransmitter := ConfigTransmitter{
		// 		Cfg: shardctrler.Config{
		// 			Num:    newCfg.Num,
		// 			Shards: newCfg.Shards,
		// 			Groups: newCfg.Groups,
		// 		},
		// 	}
		// 	// for initialization(newCfg.Num == 1), no need to pullInShards
		// 	if newCfg.Num > 1 && len(inShards) > 0 {
		// 		kv.DPrintf("new config with inShards, begin pull inShards")
		// 		cfgTransmitter.InShards = inShards
		// 		cfgTransmitter.ShardDB = make([]map[string]string, len(inShards))
		// 		cfgTransmitter.ShardPrevRequestInfo = make([]map[int64]requestInfo, len(inShards))
		// 		// TODO: consider pullInShards
		// 		kv.pullInShards(inShards, &cfgTransmitter)
		// 	}
		// 	index, _, _ := kv.rf.Start(
		// 		cfgTransmitter,
		// 	)
		// 	// kv.cfg.MyShards = newMyShards
		// 	kv.DPrintf("begin syncing new config with peers through raft, applyIndex: %v\n", index)
		// }

		kv.mu.RUnlock()

		time.Sleep(QUERY_CONFIG_INTERVAL)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(ConfigTransmitter{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.produceCount = 0
	kv.consumeCount = 0

	kv.db = map[int]*ShardDB{}
	kv.requestMap = sync.Map{}
	kv.clerkChMap = map[int64]chan applyResult{}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cfg = configInfo{
		GlobalConfig: shardctrler.Config{
			Groups: map[int][]string{},
		},
		MyShards: map[int]struct{}{},
	}

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.queryConfig()
	go kv.handleApplyMsg()

	kv.DPrintf("started\n")

	return kv
}
