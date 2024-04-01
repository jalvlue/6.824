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
	MIGRATE_SHARDS_INTERVAL = 50 * time.Millisecond
	QUERY_CONFIG_INTERVAL   = 50 * time.Millisecond
	CLERK_WAITING_TIMEOUT   = 200 * time.Millisecond
)

// debugging printer
func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[GID: %d, SVER: %d] ", kv.gid, kv.me)
		format = prefix + format
		log.Printf(format, a...)
	}
}

// ShardKV server operation types
type opType uint8

const (
	OpCommandOperation opType = iota
	OpConfiguration
	OpInitialization
	OpShardMigration
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// operation types, OpGet, OpPut, OpAppend, OpInit or OpCfg
	Type opType

	// operation content
	Params interface{}
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

	lastCfg    *shardctrler.Config
	currentCfg *shardctrler.Config

	// persister of this KVServer and its raft component
	persister *raft.Persister

	// K/V database
	// mapping: shardID -> ShardDB
	db map[int]*ShardDB

	// mapping: raftCommandIndex int -> requestResultTransmitter
	// for every clerk request to receive executed result
	requestMap sync.Map

	// mapping: clerkID int64 -> resultCh chan applyResult
	// for every clerk to transmit its request result
	// instead of create a resultCh for every single request
	clerkChMap map[int64]chan applyResult
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

	Status shardStatus
}

// create a new copy instance and return a pointer to this new instance
func (oldShard ShardDB) deepCopy() *ShardDB {
	newShard := ShardDB{
		KVMap:  map[string]string{},
		DupMap: map[int64]requestInfo{},
		Status: oldShard.Status,
	}

	for k, v := range oldShard.KVMap {
		newShard.KVMap[k] = v
	}
	for k, v := range oldShard.DupMap {
		newShard.DupMap[k] = v
	}

	return &newShard
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
	Value string
	Err   Err
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

// expect the caller to hold the lock
// check if I could serve request:
// do I own the shard && is this shard on StatusServing
func (kv *ShardKV) couldServe(shardID int) bool {
	return kv.currentCfg.Shards[shardID] == kv.gid && kv.db[shardID].Status == StatusServing
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		// kv.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	keyShardID := key2shard(args.Key)
	kv.DPrintf("receive command request from clerk [%v], args.RequestID: %v, ShardID: %v, args.CommandOp: %v, args.Key: %v, args.Value: %v\n", args.ClerkID, args.RequestID, keyShardID, args.CommandOp, args.Key, args.Value)

	kv.mu.RLock()
	// check if I could serve the request
	if !kv.couldServe(keyShardID) {
		kv.mu.RUnlock()
		kv.DPrintf("I do not own the shard or this shard is still pulling\n")

		// for shard is pulling, still return with ErrWrongGroup
		// to prevent clerk from busy request and get sometime to pull shards
		reply.Err = ErrWrongGroup
		return
	}

	// check previous request info
	if prevRequestInfo, ok := kv.db[keyShardID].DupMap[args.ClerkID]; ok && args.RequestID == prevRequestInfo.RequestID {
		kv.mu.RUnlock()

		reply.Err = prevRequestInfo.ApplyResult.Err
		reply.Value = prevRequestInfo.ApplyResult.Value

		kv.DPrintf("requestID: %v has been executed, retrieve result from previous request, args.CommandOp: %v, reply.Err: %v, reply.Value: %v\n", args.RequestID, args.CommandOp, reply.Err, reply.Value)
		return
	}
	kv.mu.RUnlock()

	index, _, _ := kv.rf.Start(
		Op{
			Type:   OpCommandOperation,
			Params: *args,
		},
	)

	kv.mu.Lock()
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
			reply.Value = res.Value
			kv.DPrintf("raft agreement finish, args.CommandOp: %v, args.Key: %v, reply.Err: %v, reply.Value: %v\n", args.CommandOp, args.Key, res.Err, res.Value)
		}
	case <-time.After(CLERK_REQUEST_INTERVAL):
		{
			kv.requestMap.Delete(index)

			reply.Err = ErrRepTimeout
			kv.DPrintf("leader reply timeout, request fail\n")
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

			if op, ok := applyMsg.Command.(Op); ok {
				kv.DPrintf("receive operation applyMsg, applyMsg.CommandIndex: %d\n", applyMsg.CommandIndex)

				switch op.Type {
				case OpInitialization:
					{
						kv.DPrintf("initial commit operation\n")
					}
				case OpConfiguration:
					{
						newCfg := op.Params.(shardctrler.Config)

						// filter unreliable duplicated config change operations
						if kv.currentCfg.Num+1 == newCfg.Num {
							kv.DPrintf("config change operation, kv.currentCfg.Num: %v\n", newCfg.Num)
							kv.lastCfg = kv.currentCfg
							kv.currentCfg = &newCfg

							// no need to pull shards in first config(newCfg.Num == 1)
							// only initialize ShardDB maps instead
							if kv.currentCfg.Num == 1 {
								for shardID := range kv.currentCfg.Shards {
									if kv.currentCfg.Shards[shardID] == kv.gid {
										kv.db[shardID] = &ShardDB{
											Status: StatusServing,
											KVMap:  map[string]string{},
											DupMap: map[int64]requestInfo{},
										}
									}
								}
							} else {
								for shardID, currentGID := range kv.currentCfg.Shards {
									if kv.lastCfg.Shards[shardID] == kv.gid && currentGID != kv.gid {

										// TODO: set status to StatusInvalid and do GC after other groups pulled this shard

										// I own this shard in lastCfg, but I lose it in currentCfg
										// set shard status to StatusBePulling and wait for other groups to pull this shard
										kv.db[shardID].Status = StatusBePulling

									} else if kv.lastCfg.Shards[shardID] != kv.gid && currentGID == kv.gid {

										// I do not own this shard in lastCfg, but I own it in currentCfg
										// set shard status to SattusPulling and wait for migrateShards goroutine to pull it from other groups
										kv.db[shardID].Status = StatusPulling

									}
								}
							}
						} else {
							kv.DPrintf("INVALID config change operation, kv.currentCfg.Num: %v, newCfg.Num: %v\n", kv.currentCfg.Num, newCfg.Num)
						}
					}
				case OpCommandOperation:
					{
						kv.DPrintf("internal DB operation\n")
						args := op.Params.(CommandArgs)

						var sender *requestResultTransmitter
						var hasClerkWaiting bool
						if entryVal, ok := kv.requestMap.Load(applyMsg.CommandIndex); ok {
							if transmitter, okk := entryVal.(requestResultTransmitter); okk && transmitter.clerkID == args.ClerkID && transmitter.requestID == args.RequestID {
								sender = &transmitter
								hasClerkWaiting = true
							}
						}

						// config change happen, this shard is no longer mine, do not execute
						if keyShardID := key2shard(args.Key); !kv.couldServe(keyShardID) {
							// return ErrWrongGroup to waiting clerk (if any)
							if hasClerkWaiting {
								sender.resultCh <- applyResult{
									Err: ErrWrongGroup,
								}
							}
						} else {
							// check and apply command
							if prevRequestInfo, ok := kv.db[keyShardID].DupMap[args.ClerkID]; !ok || args.RequestID > prevRequestInfo.RequestID {

								val, err := kv.applyCommand(&args)

								// update duplicated map
								kv.db[keyShardID].DupMap[args.ClerkID] = requestInfo{
									RequestID: args.RequestID,
									ApplyResult: applyResult{
										Value: val,
										Err:   err,
									},
								}

								// tranfer apply result back to waiting clerk (if any)
								if hasClerkWaiting {
									sender.resultCh <- applyResult{
										Value: val,
										Err:   err,
									}
								}
							}
						}
					}
				case OpShardMigration:
					{
						pulledShards := op.Params.(PullShardReply)
						// filter unreliable duplicated pulled shards in previous config
						if pulledShards.Num == kv.currentCfg.Num {
							kv.DPrintf("migrate shards operation, config Num: %v\n", pulledShards.Num)

							// pulledShards is from raft's logs and presister
							// can not reference to it
							for shardID, shardDB := range pulledShards.ShardDBs {
								if shard := kv.db[shardID]; shard.Status == StatusPulling {
									kv.DPrintf("pulled shard: %v\n", shardID)
									kv.db[shardID] = shardDB.deepCopy()
									kv.db[shardID].Status = StatusServing
								} else {
									kv.DPrintf("duplicated pulled shardID: %v\n", shardID)
									break
								}
							}
						} else {
							kv.DPrintf("pulledShards with different num, pulledShards.Num: %v, kv.currentCfg.Num: %v\n", pulledShards.Num, kv.currentCfg.Num)
						}
					}
				}
			}

			// kv.maxraftstate = -1 for no snapshot option
			// raft state out of bound, do a snapshot
			if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.generateSnapshot())
				kv.DPrintf("raft state is approaching kv.maxraftstate, snapshot created and sent to raft\n")
			}
			kv.mu.Unlock()

		} else if applyMsg.SnapshotValid {
			//snapshot msg
			kv.DPrintf("reveive snapshot applyMsg, applyMsg.SnapshotTerm: %d, applyMsg.SnapshotIndex: %d\n", applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)

			kv.mu.Lock()
			kv.applySnapshot(applyMsg.Snapshot)
			// TODO: consider store lastCfg in snapshot or do a query
			// lastCfg := kv.mck.Query(kv.currentCfg.Num - 1)
			// kv.lastCfg = &lastCfg
			kv.DPrintf("read and apply snapshot success, kv.currentCfg: %v\n", kv.currentCfg)
			kv.mu.Unlock()
		}
	}
}

// expect the caller to hold the lock
// executed internal db operations
func (kv *ShardKV) applyCommand(args *CommandArgs) (string, Err) {
	kv.DPrintf("apply db operation, args.Op: %v, args.Key: %v, args.Value: %v\n", args.CommandOp, args.Key, args.Value)
	switch args.CommandOp {
	case CommandGet:
		return kv.get(args.Key)
	case CommandPut:
		return kv.put(args.Key, args.Value)
	case CommandAppend:
		return kv.append(args.Key, args.Value)
	default:
		return "", ErrDefault
	}
}

type KVSnapshot struct {
	DB         map[int]*ShardDB
	CurrentCfg shardctrler.Config
	LastCfg    shardctrler.Config
}

// expect the caller to hold the lock
// generate a snapshot of server storage: kv.db
// and detect duplicated operations state: kv.clerkPrevRequestInfo
func (kv *ShardKV) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot := KVSnapshot{
		DB:         kv.db,
		CurrentCfg: *kv.currentCfg,
		LastCfg:    *kv.lastCfg,
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
		kv.currentCfg = &snapshot.CurrentCfg
		kv.lastCfg = &snapshot.LastCfg
	}
}

// long-running go routine asking shardctrler for latest config every 100ms
func (kv *ShardKV) queryConfig() {

	// restore state
	// commit a init(empty) log
	if !kv.rf.HasLogInCurrentTerm() {
		kv.DPrintf("init command\n")
		kv.rf.Start(Op{
			Type: OpInitialization,
		})
		return
	}

	// only poll for new config if all my shards in current config is all serving
	// i.e. only try to update to new version config after current config is settled
	couldQueryNewCfg := true

	kv.mu.RLock()
	for shardID, shardDB := range kv.db {
		if kv.currentCfg.Shards[shardID] == kv.gid && shardDB.Status != StatusServing {
			couldQueryNewCfg = false
			break
		}
	}

	myCurrentCfgNum := kv.currentCfg.Num
	kv.mu.RUnlock()

	if couldQueryNewCfg {
		if newCfg := kv.mck.Query(myCurrentCfgNum + 1); newCfg.Num == myCurrentCfgNum+1 {

			// commit a configuration log once there is new config
			index, _, _ := kv.rf.Start(Op{
				Type:   OpConfiguration,
				Params: newCfg,
			})
			kv.DPrintf("new config found, config [%v]: %v, sync config through raft begin, applyIndex: %v\n", newCfg.Num, newCfg, index)
		}
	}
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	// only the leader could migrate shards to other groups
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.DPrintf("receive PullShard request in config Num: %v for shards: %v\n", args.Num, args.ShardIDs)
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// I have lower config Num, return with ErrWrongNum, and wait for my config update
	if kv.currentCfg.Num < args.Num {
		kv.DPrintf("ErrWrongNum, args.Num: %v, kv.currentCfg.Num: %v\n", args.Num, kv.currentCfg.Num)
		reply.Err = ErrWrongNum
		return
	}

	reply.ShardDBs = map[int]ShardDB{}
	for _, shardID := range args.ShardIDs {
		reply.ShardDBs[shardID] = *(kv.db[shardID].deepCopy())
	}

	reply.Err = OK
	reply.Num = args.Num
}

// pull shards from other groups (if any)
func (kv *ShardKV) migrateShards() {

	kv.mu.RLock()

	// mapping: lastGID -> []int shards that I need to pull
	gid2ShardIDs := map[int][]int{}
	for shardID := range kv.currentCfg.Shards {
		if kv.db[shardID].Status == StatusPulling {
			lastGID := kv.lastCfg.Shards[shardID]
			kv.DPrintf("for pulling shard: %v, lastGID: %v\n", shardID, lastGID)
			if shards, ok := gid2ShardIDs[lastGID]; ok {
				shards = append(shards, shardID)
				gid2ShardIDs[lastGID] = shards
			} else {
				gid2ShardIDs[lastGID] = []int{shardID}
			}
		}
	}

	// TODO: still has some bugs when the network is unreliable
	var wg sync.WaitGroup
	for gid, inShardIDs := range gid2ShardIDs {
		wg.Add(1)
		kv.DPrintf("begin PullShard RPC for shards: %v from GID: %v in config Num: %v\n", inShardIDs, gid, kv.currentCfg.Num)
		go func(gid int, servers []string, configNum int, shardIDs []int) {
			defer wg.Done()

			args := PullShardArgs{
				Num:      configNum,
				ShardIDs: shardIDs,
			}
			for i := range servers {
				var reply PullShardReply
				if ok := kv.make_end(servers[i]).Call("ShardKV.PullShard", &args, &reply); ok && reply.Err == OK {
					// kv.DPrintf("DEBUG pulledShards: %v\n", reply)
					index, _, _ := kv.rf.Start(Op{
						Type:   OpShardMigration,
						Params: reply,
					})
					kv.DPrintf("receive PullShard response from GID: %v, server: %v, get shards: %v in configNum: %v, applyIndex: %v\n", gid, i, shardIDs, configNum, index)
					return
				} else {
					kv.DPrintf("PullShard request to GID: %v, server: %v fail\n", gid, i)
				}
			}
		}(gid, kv.lastCfg.Groups[gid], kv.currentCfg.Num, inShardIDs)
	}

	kv.mu.RUnlock()
	wg.Wait()
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

// long-running goroutines doing actions that only leader need to do
func (kv *ShardKV) daemon(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(CommandArgs{})
	labgob.Register(PullShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.db = map[int]*ShardDB{}
	kv.requestMap = sync.Map{}
	kv.clerkChMap = map[int64]chan applyResult{}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.currentCfg = &shardctrler.Config{
		Groups: map[int][]string{},
	}
	kv.lastCfg = kv.currentCfg

	for shardID := range kv.currentCfg.Shards {
		kv.db[shardID] = &ShardDB{
			Status: StatusInvalid,
		}
	}

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.daemon(kv.queryConfig, QUERY_CONFIG_INTERVAL)
	go kv.daemon(kv.migrateShards, MIGRATE_SHARDS_INTERVAL)
	go kv.handleApplyMsg()

	kv.DPrintf("started\n")

	return kv
}
