package shardctrler

import (
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	// longest time a clerk waiting for raft leader to reach agreement
	CLERK_WAITING_TIMEOUT = 200 * time.Millisecond

	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"
)

// debugging printer
func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("SCTL [%d] ", sc.me)
		format = prefix + format
		log.Printf(format, a...)
	}
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// request count the producer(Clerk) has made
	// process count the consumer(ShardCtrler) has made
	produceCount int
	consumeCount int

	// mapping: raftCommandIndex -> requestResultTransmitter
	requestMap sync.Map

	// TODO: maybe combine clerkPrevRequestInfo and clerkChMap
	// mapping: clerkID -> prevExecutedResult
	clerkPrevRequestInfo map[int64]requestInfo
	// mapping: clerkID -> clerk chan
	clerkChMap map[int64]chan interface{}

	// mapping: GID -> shardLoads in the latest config
	groupLoads map[int]int
}

// id and result of last request the clerk made
// for duplicated operations detection
type requestInfo struct {
	RequestID int64
	Result    interface{}
}

// passing executed result between sc.handleApplyMsg and sc.Join/Leave/Move/Query RPC through resultCh
type requestResultTransmitter struct {
	clerkID   int64
	requestID int64

	// reference to kv.clerkChMap[clerkID]
	resultCh chan interface{}
}

type Op struct {
	// Your data here.

	// operation types: OpJoin, OpLeave, OpMove or OpQuery
	Type string

	// operation content
	// for OpJoin, len(Params) = 1, Params[0] = newGroups
	// for OpLeave, len(Params) = 1, Params[0] = leaveServerGIDs
	// for OpMove, len(Params) = 2, Params[0] = Shard, Params[1] = GID
	// for OpQuery, len(Params) = 1, Params[0] = configNum
	Params []interface{}

	ClerkID   int64
	RequestID int64
}

// get a copy of a config
func (sc *ShardCtrler) copyConfig(cfg *Config) Config {
	cpy := Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: map[int][]string{},
	}

	for k, v := range cfg.Groups {
		cpy.Groups[k] = v
	}

	return cpy
}

// get a copy of latest config
func (sc *ShardCtrler) getLastConfig() Config {
	return sc.copyConfig(&sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	// for debug
	gids := mapToSlice(args.Servers)
	sc.DPrintf("receive Join RPC from clerk [%v], args.RequestID: \"%v\", args.Gids: %v\n", args.ClerkID, args.RequestID, gids)

	// check duplication
	sc.mu.RLock()
	prevRequestInfo, ok := sc.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		sc.mu.RUnlock()

		reply.Err = OK

		sc.DPrintf("requestID: \"%v\" has been executed\n")
		return
	}
	sc.mu.RUnlock()

	// ask raft to reach consensus
	index, _, _ := sc.rf.Start(
		Op{
			Type:      OpJoin,
			Params:    []interface{}{args.Servers},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

	// prepare to receive raft consensus reply
	sc.mu.Lock()
	sc.produceCount += 1
	resultCh, ok := sc.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan interface{})
		sc.clerkChMap[args.ClerkID] = resultCh
	}
	sc.mu.Unlock()

	sc.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	sc.requestMap.Store(index, receiver)

	// wait for raft reply
	select {
	case <-receiver.resultCh:
		{
			sc.requestMap.Delete(index)

			reply.Err = OK

			sc.DPrintf("raft agreement finish\n")
		}
	case <-time.After(CLERK_WAITING_TIMEOUT):
		{
			sc.requestMap.Delete(index)

			reply.Err = ErrRepTimeout
			sc.DPrintf("leader reply timeout, request fail\n")

			sc.mu.Lock()
			sc.consumeCount += 1
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	sc.DPrintf("receive Leave RPC from clerk [%v], args.RequestID: \"%v\", args.Gids: %v\n", args.ClerkID, args.RequestID, args.GIDs)

	sc.mu.RLock()
	prevRequestInfo, ok := sc.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		sc.mu.RUnlock()

		reply.Err = OK

		sc.DPrintf("requestID: \"%v\" has been executed\n", args.RequestID)
		return
	}
	sc.mu.RUnlock()
	index, _, _ := sc.rf.Start(
		Op{
			Type:      OpLeave,
			Params:    []interface{}{args.GIDs},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

	sc.mu.Lock()
	sc.produceCount += 1
	resultCh, ok := sc.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan interface{})
		sc.clerkChMap[args.ClerkID] = resultCh
	}
	sc.mu.Unlock()

	sc.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	sc.requestMap.Store(index, receiver)

	select {
	case <-receiver.resultCh:
		{
			sc.requestMap.Delete(index)

			reply.Err = OK

			sc.DPrintf("raft agreement finish\n")
		}
	case <-time.After(CLERK_WAITING_TIMEOUT):
		{
			sc.requestMap.Delete(index)
			reply.Err = ErrRepTimeout
			sc.DPrintf("leader reply timeout, request fail\n")

			sc.mu.Lock()
			sc.consumeCount += 1
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	sc.DPrintf("receive Move RPC from clerk [%v], args.RequestID: \"%v\", args.GID: %v, args.Shard: %v\n", args.ClerkID, args.RequestID, args.GID, args.Shard)

	sc.mu.RLock()
	prevRequestInfo, ok := sc.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		sc.mu.RUnlock()

		reply.Err = OK

		sc.DPrintf("requestID: \"%v\" has been executed\n")
		return
	}
	sc.mu.RUnlock()

	index, _, _ := sc.rf.Start(
		Op{
			Type:      OpMove,
			Params:    []interface{}{args.Shard, args.GID},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

	sc.mu.Lock()
	sc.produceCount += 1
	resultCh, ok := sc.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan interface{})
		sc.clerkChMap[args.ClerkID] = resultCh
	}
	sc.mu.Unlock()

	sc.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	sc.requestMap.Store(index, receiver)

	select {
	case <-receiver.resultCh:
		{
			sc.requestMap.Delete(index)

			reply.Err = OK

			sc.DPrintf("raft agreement finish\n")
		}
	case <-time.After(CLERK_WAITING_TIMEOUT):
		{
			sc.requestMap.Delete(index)
			reply.Err = ErrRepTimeout
			sc.DPrintf("leader reply timeout, request fail\n")

			sc.mu.Lock()
			sc.consumeCount += 1
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("I am not leader, return with ErrWrongLeader\n")
		return
	}

	sc.DPrintf("receive Query RPC from clerk [%v], args.RequestID: \"%v\", args.Num: %v\n", args.ClerkID, args.RequestID, args.Num)

	sc.mu.RLock()
	prevRequestInfo, ok := sc.clerkPrevRequestInfo[args.ClerkID]
	if ok && args.RequestID == prevRequestInfo.RequestID {
		sc.mu.RUnlock()

		reply.Err = OK
		reply.Config = prevRequestInfo.Result.(Config)

		sc.DPrintf("requestID: \"%v\" has been executed\n", args.RequestID)
		return
	}
	sc.mu.RUnlock()

	index, _, _ := sc.rf.Start(
		Op{
			Type:      OpQuery,
			Params:    []interface{}{args.Num},
			ClerkID:   args.ClerkID,
			RequestID: args.RequestID,
		},
	)

	sc.mu.Lock()
	sc.produceCount += 1
	resultCh, ok := sc.clerkChMap[args.ClerkID]
	if !ok {
		resultCh = make(chan interface{})
		sc.clerkChMap[args.ClerkID] = resultCh
	}
	sc.mu.Unlock()

	sc.DPrintf("raft agreement start, applyIndex: {%d}\n", index)

	receiver := requestResultTransmitter{
		clerkID:   args.ClerkID,
		requestID: args.RequestID,
		resultCh:  resultCh,
	}
	sc.requestMap.Store(index, receiver)

	select {
	case res := <-receiver.resultCh:
		{
			sc.requestMap.Delete(index)

			reply.Err = OK
			reply.Config = res.(Config)

			sc.DPrintf("raft agreement finish\n")
		}
	case <-time.After(CLERK_WAITING_TIMEOUT):
		{
			sc.requestMap.Delete(index)

			reply.Err = ErrRepTimeout
			sc.DPrintf("leader reply timeout, request fail\n")

			sc.mu.Lock()
			sc.consumeCount += 1
			sc.mu.Unlock()
		}
	}
}

// long-running listen to raft message from applyCh
func (sc *ShardCtrler) handleApplyMsg() {
	for {
		applyMsg := <-sc.applyCh
		{
			if applyMsg.CommandValid {
				// command msg
				sc.DPrintf("receive command commit applyMsg, applyMsg.CommandIndex: %d\n", applyMsg.CommandIndex)

				sc.mu.Lock()
				if op, ok := applyMsg.Command.(Op); ok {
					// check and apply command
					if prevRequestInfo, okk := sc.clerkPrevRequestInfo[op.ClerkID]; !okk || op.RequestID > prevRequestInfo.RequestID {
						res := sc.applyCommand(&applyMsg, &op)
						sc.clerkPrevRequestInfo[op.ClerkID] = requestInfo{
							RequestID: op.RequestID,
							Result:    res,
						}

						// check and send executed result back to request
						if requestToConsumeNum := sc.produceCount - sc.consumeCount; requestToConsumeNum > 0 {
							if entryVal, okkk := sc.requestMap.Load(applyMsg.CommandIndex); okkk {
								if sender, okkkk := entryVal.(requestResultTransmitter); okkkk && sender.clerkID == op.ClerkID && sender.requestID == op.RequestID {
									sc.consumeCount += 1
									sender.resultCh <- res
								}
							}
						} else if requestToConsumeNum == 0 {
							sc.produceCount = 0
							sc.consumeCount = 0
						}
					}
				}
				sc.mu.Unlock()
			}
		}
	}
}

// reassign shards for balanced load when newGroups join
func (sc *ShardCtrler) reassignGroupJoin(shards *[NShards]int, newGroups, oldGroups []int) {
	numNewGroup := len(newGroups)
	numOldGroups := len(oldGroups)

	numAvailableGroups := float64(numNewGroup + numOldGroups)
	shardTasks := float64(NShards)
	availableGroupAverageLoad := shardTasks / numAvailableGroups

	// sort newGroups by loads and GIDs just for consistency between peers
	sortGroupByLoads(newGroups, sc.groupLoads, 0, numNewGroup-1, isGreater)

	// first boot without joined groups
	if numOldGroups == 0 {
		sc.DPrintf("first boot without joined groups")
		for index := 0; index < NShards; index++ {
			newGID := newGroups[index%numNewGroup]
			shards[index] = newGID
			sc.groupLoads[newGID] += 1
		}

		return
	}

	// sc.DPrintf("before sort: [GID, Load]\n")
	// for _, gid := range oldGroups {
	// 	sc.DPrintf("[%v, %v] ", gid, sc.groupLoads[gid])
	// }
	// sc.DPrintf("\n")

	// sort oldGroups by loads and GIDs just for consistency between peers
	sortGroupByLoads(oldGroups, sc.groupLoads, 0, numOldGroups-1, isGreater)

	// sc.DPrintf("after sort: [GID, Load]\n")
	// for _, gid := range oldGroups {
	// 	sc.DPrintf("[%v, %v] ", gid, sc.groupLoads[gid])
	// }
	// sc.DPrintf("\n")

	index := -1
	for _, oldGID := range oldGroups {
		sc.DPrintf("balancing oldGID: %d, groupLoad: %v\n", oldGID, sc.groupLoads[oldGID])
		for needToMove(sc.groupLoads[oldGID], availableGroupAverageLoad) {

			// new groups accept loads in a round turn for balanced load
			index += 1
			newGID := newGroups[index%numNewGroup]
			moveOneShardTo(shards, oldGID, newGID)
			sc.DPrintf("move one shard of oldGID: [%v] to newGID [%v]\n", oldGID, newGID)

			sc.groupLoads[oldGID] -= 1
			sc.groupLoads[newGID] += 1
		}

		// finish balancing load of a old group
		shardTasks -= float64(sc.groupLoads[oldGID])
		numAvailableGroups -= 1
		availableGroupAverageLoad = shardTasks / numAvailableGroups

		sc.DPrintf("shardTasks: %v, numAvailableGroups: %v, availableGroupAverageLoad: %v, sc.groupLoad[oldGID] = %v\n", shardTasks, numAvailableGroups, availableGroupAverageLoad, sc.groupLoads[oldGID])
	}
}

// reassign shards for balanced load when leaveGroups leave
func (sc *ShardCtrler) reassignGroupsLeave(shards *[NShards]int, leaveGroups, remainGroups []int) {
	sc.DPrintf("leaveGroup: %v, remainGroup: %v\n", leaveGroups, remainGroups)

	numRemainGroups := len(remainGroups)
	if numRemainGroups == 0 {
		for _, leaveGID := range leaveGroups {
			delete(sc.groupLoads, leaveGID)
		}
		return
	}

	// sc.DPrintf("before sort: [GID, Load]\n")
	// for _, gid := range remainGroups {
	// 	sc.DPrintf("[%v, %v] ", gid, sc.groupLoads[gid])
	// }
	// sc.DPrintf("\n")

	// sort group by loads from low load to high load
	// so that groups with low loads may receive more shards and achieve better load balance
	sortGroupByLoads(remainGroups, sc.groupLoads, 0, numRemainGroups-1, isLess)

	// sc.DPrintf("after sort: [GID, Load]\n")
	// for _, gid := range remainGroups {
	// 	sc.DPrintf("[%v, %v] ", gid, sc.groupLoads[gid])
	// }
	// sc.DPrintf("\n")

	index := -1
	for _, leaveGID := range leaveGroups {

		// reassign leaveGroup's loads to remainGroups
		leaveLoad := sc.groupLoads[leaveGID]
		sc.DPrintf("leaveGID: %v, leaveLoad: %v\n", leaveGID, leaveLoad)
		for i := 0; i < leaveLoad; i++ {
			index += 1
			remainGID := remainGroups[index%numRemainGroups]
			moveOneShardTo(shards, leaveGID, remainGID)
			sc.DPrintf("move one shard of leaveGID: [%v] to remainGID [%v]\n", leaveGID, remainGID)
			sc.groupLoads[remainGID] += 1
		}

		// finish reassign load of a leaveGroup
		delete(sc.groupLoads, leaveGID)
	}
}

// expect the caller to hold the lock
// apply commands from applyCh sent by raft component
func (sc *ShardCtrler) applyCommand(applyMsg *raft.ApplyMsg, op *Op) interface{} {
	sc.DPrintf("apply command commit applyMsg, applyMsg.Index: %d, applyMsg.Op: %v", applyMsg.CommandIndex, op)

	if op.Type == OpJoin {

		// a copy of lastConfig, so we can change its fields and reference to it safely
		cfg := sc.getLastConfig()
		cfg.Num += 1

		newGroups := op.Params[0].(map[int][]string)

		sc.reassignGroupJoin(&cfg.Shards, mapToSlice(newGroups), mapToSlice(cfg.Groups))

		for k, v := range newGroups {
			cfg.Groups[k] = v
		}

		sc.DPrintf("new config apply, config Num: %v, config Shards: %v, config Groups: %v\n", cfg.Num, cfg.Shards, mapToSlice(cfg.Groups))
		sc.configs = append(sc.configs, cfg)
		return nil

	} else if op.Type == OpLeave {

		cfg := sc.getLastConfig()
		cfg.Num += 1

		leaveGroups := op.Params[0].([]int)
		for _, leaveGID := range leaveGroups {
			delete(cfg.Groups, leaveGID)
		}

		sc.reassignGroupsLeave(&cfg.Shards, leaveGroups, mapToSlice(cfg.Groups))
		sc.DPrintf("new config apply, config Num: %v, config Shards: %v, config Groups: %v\n", cfg.Num, cfg.Shards, mapToSlice(cfg.Groups))

		sc.configs = append(sc.configs, cfg)
		return nil

	} else if op.Type == OpMove {

		shard := op.Params[0].(int)
		newGID := op.Params[1].(int)

		cfg := sc.getLastConfig()
		cfg.Num += 1

		originGID := cfg.Shards[shard]
		sc.groupLoads[originGID] -= 1

		cfg.Shards[shard] = newGID
		sc.groupLoads[newGID] += 1

		sc.DPrintf("new config apply, config Num: %v, config Shards: %v, config Groups: %v\n", cfg.Num, cfg.Shards, mapToSlice(cfg.Groups))
		sc.configs = append(sc.configs, cfg)

		return nil

	} else if op.Type == OpQuery {

		num := op.Params[0].(int)
		if num < 0 || num >= len(sc.configs) {
			return sc.getLastConfig()
		}

		return sc.copyConfig(&sc.configs[num])

	} else {
		// ignore other kinds of operations
		return nil
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.produceCount = 0
	sc.consumeCount = 0
	sc.requestMap = sync.Map{}
	sc.clerkPrevRequestInfo = map[int64]requestInfo{}
	sc.clerkChMap = map[int64]chan interface{}{}
	sc.groupLoads = map[int]int{}

	go sc.handleApplyMsg()

	return sc
}
