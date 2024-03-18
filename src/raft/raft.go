package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER           = byte(0)
	CANDIDATE          = byte(1)
	LEADER             = byte(2)
	HEARTHEAT_INTERVAL = 100 * time.Millisecond
	NO_VOTE            = -1
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex     int
	lastApplied     int
	status          byte
	applyChan       chan ApplyMsg
	electionTimeout time.Duration
	electionTime    time.Time

	// volatile state on leaders
	// for each server, index of the next log entry
	// to send to that server
	nextIndex []int

	// for each server, index of highest log entry
	// known to be replicated on server
	matchIndex []int

	// index of first client request to this leader
	// -1 as default, with rf.firstIndex == -1,
	// this leader would not commit logs which appended by former leaders
	firstIndex           int
	heartbeatTime        time.Time
	triggerAE            bool
	leaderHeartbeatCount int

	// snapshot of the server, lastIndex info in that snapshot
	// (0, 0) for nil snapshot
	snapshot          []byte
	lastIncludedTerm  int
	lastIncludedIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// expect the caller to hold the lock
// get logic index, term of last log
func (rf *Raft) lastLogInfo() (int, int) {
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLogEntryTerm(lastLogIndex)
	return lastLogIndex, lastLogTerm
}

// expect the caller to hold the lock
func (rf *Raft) getSlicePosition(logicIndex int) int {
	return logicIndex - rf.lastIncludedIndex - 1
}

// expect the caller to hold the lock
// IMPORTANT: try to add rf.lastIndex field instead of calculate it,
// but the performance is 2 times slower than the original implementation
func (rf *Raft) getLastIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

// expect the caller to hold the lock
func (rf *Raft) getLogEntryTerm(index int) int {
	index -= rf.lastIncludedIndex

	// the index is 0 or is snapshoted as lastIncludedIndex
	if index == 0 {
		return rf.lastIncludedTerm
	}

	// the index is snapshoted and not the lastIncludedIndex
	// happends in unreliable out-of-date requests
	if index < 0 {
		return -2
	}

	// do not have the index-th log entry
	if index > len(rf.log) {
		return -1
	}

	return rf.log[index-1].Term
}

// expect the caller to hold the lock
func (rf *Raft) getLogEntryCommand(index int) interface{} {
	index -= rf.lastIncludedIndex
	return rf.log[index-1].Command
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, (rf.status == LEADER)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// expect the caller to hold the lock
	// rf.mu.RLock()
	// defer rf.mu.RUnlock()

	// start := time.Now()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// persister.Save would just clone a copy of args
	// so there is not need to create a new copy of log
	// pLog := make([]LogEntry, len(rf.log))
	// copy(pLog, rf.log)

	persistState := persistStates{
		Term:              rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}

	if err := e.Encode(persistState); err != nil {
		panic(fmt.Errorf("persist encode fail: %w", err))
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)

	// elapsed := time.Since(start)
	// test: write persist would take 1~3ms
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	rf.DPrintf(dPersist, "write persist success, currentTerm: %d, votedFor:%d, lastLogTerm: %d, lastLogIndex: %d\n", rf.currentTerm, rf.votedFor, lastLogTerm, lastLogIndex)
}

type persistStates struct {
	Term              int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// restore previously persisted state.
func (rf *Raft) readPersist(state, snapshot []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// start := time.Now()

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var ps persistStates

	if err := d.Decode(&ps); err != nil {
		panic(fmt.Errorf("persist decode fail: %w", err))
	} else {
		rf.currentTerm = ps.Term
		rf.votedFor = ps.VotedFor

		// rf.log = make([]LogEntry, len(ps.Log))
		// copy(rf.log, ps.Log)

		// The data, which comes from persister.ReadRaftState
		// is a copy of persister.raftState
		// so just point rf.log to ps.Log
		// since it is a copy
		rf.log = ps.Log

		rf.lastIncludedTerm = ps.LastIncludedTerm
		rf.lastIncludedIndex = ps.LastIncludedIndex
	}

	// @Param snapshot is a copy from rf.persister
	// so we can reference it safely
	rf.snapshot = snapshot
	rf.commitIndex = rf.lastIncludedIndex

	// send snapshot (if any) to rf.applyChan in other goroutine
	// to avoid deadlock( Make() --> rf.readPersist() --> return rf --> setup rf.applyChan --> Make() )
	if len(snapshot) != 0 {
		go func() {
			// everything passed in a literal expression would be copy
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.DPrintf(dSnap, "send snapshot to service, snapshotTerm: %d, snapshotIndex: %d\n", rf.lastIncludedTerm, rf.lastIncludedIndex)

			rf.applyChan <- ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
		}()
	}

	// elapsed := time.Since(start)
	// test: the process of reading persist would take 1~3ms
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	rf.DPrintf(dPersist, "read persist success, currentTerm: %d, votedFor:%d, lastLogTerm: %d, lastLogIndex: %d\n", rf.currentTerm, rf.votedFor, lastLogTerm, lastLogIndex)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	// the caller(tester or service) would only call rf.Snapshot
	// while sending log commit to rf.applyCh,
	// i.e. the caller would hold the lock
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// the caller just simply notice rf that
	// it has created a snapshot,
	// so we can use these infomations to trim logs later
	// instead of immediately which would cause data race.
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// out-of-date snapshot
		if index < rf.lastIncludedIndex {
			return
		}

		rf.DPrintf(dSnap, "snapshot created, lastIncludedIndex: {%d}\n", index)

		rf.lastIncludedTerm = rf.getLogEntryTerm(index)
		rf.log = rf.log[index-rf.lastIncludedIndex:]
		rf.lastIncludedIndex = index
		rf.snapshot = snapshot
		rf.persist()
	}()
}

type InstallSnapshotArgs struct {
	LeaderTerm        int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
	LogEntries        []LogEntry
}

type InstallSnapshotReply struct {
	Term              int
	FollowerLastIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	doPersist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	rf.DPrintf(dSnap, "receive InstallSnapshot RPC from leader [%d], args.LastIncludedTerm: %d, args.LastIncludedIndex: %d, args.LogEntriesLength: %d\n", args.LeaderID, args.LastIncludedTerm, args.LastIncludedIndex, len(args.LogEntries))

	if rf.currentTerm > args.LeaderTerm {
		rf.DPrintf(dLog, "in a higher term, ignore InstallSnapshot RPC and notify leader [%d]\n", args.LeaderID)
		return
	} else if rf.currentTerm == args.LeaderTerm {
		if rf.status != FOLLOWER {
			rf.status = FOLLOWER
		}
		rf.resetElectionTime()
	} else {
		rf.DPrintf(dLog, "in a lower term, convert to follower\n")
		rf.setFollower(args.LeaderTerm, NO_VOTE)
		rf.resetElectionTime()
		go rf.electionTicker(rf.currentTerm)

		doPersist = true
		reply.Term = rf.currentTerm
	}

	myLastIndex, myLastTerm := rf.lastLogInfo()
	rf.DPrintf(dSnap, "myLastIndex: %d, myLastTerm: %d\n", myLastIndex, myLastTerm)
	if myLastTerm < args.LastIncludedTerm ||
		(myLastTerm == args.LastIncludedTerm && myLastIndex < args.LastIncludedIndex) {
		rf.snapshot = args.Snapshot
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex

		rf.log = []LogEntry{}
		rf.log = append(rf.log, args.LogEntries...)

		doPersist = true

		rf.applyChan <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}

		rf.DPrintf(dSnap, "install snapshot success, myLastIncludedTerm: %d, myLastIncludedIndex: %d, myLastIndex: %d, myCommitIndex: %d\n", rf.lastIncludedTerm, rf.lastIncludedIndex, rf.getLastIndex(), rf.commitIndex)
	} else {
		rf.DPrintf(dSnap, "I do have logs newer than args.LastIncludedIndex\n")
		if myLastIndex < (args.LastIncludedIndex + len(args.LogEntries)) {
			if rf.lastIncludedIndex < args.LastIncludedIndex {
				rf.log = rf.log[0 : rf.getSlicePosition(args.LastIncludedIndex)+1]
				rf.log = append(rf.log, args.LogEntries...)
			} else {
				rf.log = []LogEntry{}
				rf.log = append(rf.log, args.LogEntries[rf.lastIncludedIndex-args.LastIncludedIndex:]...)
			}

			doPersist = true
			rf.DPrintf(dSnap, "copy logEntries success, myLastIndex: %d\n", args.LastIncludedIndex+len(args.LogEntries))
		}
	}

	reply.FollowerLastIndex = rf.getLastIndex()

	if doPersist {
		rf.persist()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendIStoPeer(server int) {
	rf.mu.Lock()

	snapshotToSend := make([]byte, len(rf.snapshot))
	copy(snapshotToSend, rf.snapshot)
	logsEntriesToSend := make([]LogEntry, len(rf.log))
	copy(logsEntriesToSend, rf.log)

	args := &InstallSnapshotArgs{
		LeaderTerm:        rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          snapshotToSend,
		LogEntries:        logsEntriesToSend,
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.setFollower(reply.Term, NO_VOTE)
			rf.resetElectionTime()
			go rf.electionTicker(rf.currentTerm)

			rf.DPrintf(dTimer, "lagging behind other peers, install snapshot abort, convert to follower\n")

			rf.persist()
			return
		}

		rf.DPrintf(dSnap, "receive InstallSnapshot RPC reply from [%d], reply.Term: %d, reply.FollowerLastIndex: %v\n", server, reply.Term, reply.FollowerLastIndex)

		if reply.Term < rf.currentTerm {
			rf.DPrintf(dDrop, "reply from previous term, discard reply\n")
			return
		}

		if reply.FollowerLastIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = reply.FollowerLastIndex
			rf.nextIndex[server] = reply.FollowerLastIndex + 1
		}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm         int
	CandidateID           int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	Status      byte
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf(dVote, "receive RequestVoteRPC from [%d]\n", args.CandidateID)

	reply.Term = rf.currentTerm
	reply.Status = rf.status
	reply.VoteGranted = false

	// in a higher term
	// no vote granted
	if rf.currentTerm > args.CandidateTerm {
		rf.DPrintf(dVote, "in a higher term, no vote granted to [%d]\n", args.CandidateID)
		return
	}

	doPersist := false

	// in a lower term
	// convert to a follower of this term but no vote yet,
	// check if vote to candidate depends on logs later
	if rf.currentTerm < args.CandidateTerm {
		rf.setFollower(args.CandidateTerm, NO_VOTE)
		doPersist = true
		reply.Status = FOLLOWER
		rf.resetElectionTime()
		go rf.electionTicker(rf.currentTerm)
		rf.DPrintf(dVote, "in a lower term, convert to follower of this term\n")
	}

	myLastLogIndex, myLastLogTerm := rf.lastLogInfo()

	// vote granted if no vote yet and have out-of-date logs than the candidate
	if rf.votedFor == NO_VOTE || rf.votedFor == args.CandidateID {
		if args.CandidateLastLogTerm > myLastLogTerm ||
			(args.CandidateLastLogTerm == myLastLogTerm && args.CandidateLastLogIndex >= myLastLogIndex) {
			rf.DPrintf(dVote, "vote granted to [%d]\n", args.CandidateID)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			doPersist = true
			rf.resetElectionTime()
		} else {
			rf.DPrintf(dVote, "has newer logs, no vote granted to candidate [%d]", args.CandidateID)
		}
	} else {
		rf.DPrintf(dDrop, "vote to candidate [%d] in this term, no vote granted to candidate [%d]\n", rf.votedFor, args.CandidateID)
	}

	if doPersist {
		rf.persist()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderID   int

	// highest log entry index right before
	// the new LogEntries sending in this AE
	LeaderPrevLogIndex int

	// term of PrevLogIndex entry
	LeaderPrevLogTerm int

	// entries sending at this AE
	// empty for heartbeat
	LogEntries []LogEntry

	// index of leader's highest committed log entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// follower's term for leader to update itself
	Term int

	// true if follower contained entry matching LeaderPrevLogIndex and LeaderPrevLogTerm,
	// i.e. logs in this AE have been replicated by the follower or not
	Success bool

	// term in the conflicting entry
	XTerm int

	// index of first entry with that term
	XIndex int

	// length of follower's log
	XLen int
}

func (rf *Raft) checkConflict(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// conflict occur: same index but with different terms
	if myPrevLogTerm := rf.getLogEntryTerm(args.LeaderPrevLogIndex); myPrevLogTerm != args.LeaderPrevLogTerm {
		// follower's log is too short for LeaderPrevLogIndex
		if myPrevLogTerm == -1 {
			reply.XLen = rf.getLastIndex()
			return
		}

		var index int
		for index = args.LeaderPrevLogIndex - 1; index > rf.lastIncludedIndex && rf.getLogEntryTerm(index) == myPrevLogTerm; index-- {
		}

		reply.XTerm = myPrevLogTerm
		reply.XIndex = index + 1
	} else {
		// not conflict, the follower has log entry of index LeaderPrevLogIndex
		reply.Success = true
	}
}

// append entries and heartbeat RPC
// for lab 2A only heartbeat is implemented
// for lab 2B log replication is implemented
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	doPersist := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf(dLog, "receive heartbeat RPC from leader [%d], LeaderTerm: %d, LeaderPrevLogIndex: %d, LeaderPrevLogTerm: %d, LeaderCommit: %d", args.LeaderID, args.LeaderTerm, args.LeaderPrevLogIndex, args.LeaderPrevLogTerm, args.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = 0
	reply.XIndex = 0
	reply.XLen = -1

	if rf.currentTerm > args.LeaderTerm {
		// in a higher term
		rf.DPrintf(dLog, "in a higher term, ignore heartbeat and notify leader [%d]\n", args.LeaderID)
		return
	} else if rf.currentTerm == args.LeaderTerm {
		// in the same term
		if rf.status != FOLLOWER {
			rf.status = FOLLOWER
		}
		rf.resetElectionTime()
	} else {
		// in a lower term
		// convert to follower of this term

		rf.DPrintf(dLog, "in a lower term, convert to follower\n")
		rf.setFollower(args.LeaderTerm, NO_VOTE)
		rf.resetElectionTime()
		go rf.electionTicker(rf.currentTerm)

		doPersist = true
		reply.Term = rf.currentTerm
	}

	// heartbeat without logs
	if args.LogEntries == nil {
		rf.DPrintf(dLog, "heartbeat without logs")
		rf.checkConflict(args, reply)
		if reply.Success {
			rf.DPrintf(dLog, "I do have logs before args.LeaderPrevLogIndex")
		}
	} else {
		// heartbeat with logs
		argsEntriesLen := len(args.LogEntries)
		leaderArgsLastLogTerm := args.LogEntries[argsEntriesLen-1].Term
		leaderArgsLastLogIndex := args.LeaderPrevLogIndex + argsEntriesLen

		// quick return since the follower already
		// have all logs in this AE
		// i.e. this AE is out-of-date
		if myArgsLastLogTerm := rf.getLogEntryTerm(leaderArgsLastLogIndex); myArgsLastLogTerm == leaderArgsLastLogTerm {
			reply.Success = true
			rf.DPrintf(dLog, "I do have receive newer heartbeats, and own all logs of this AE")
		} else {

			rf.checkConflict(args, reply)

			if reply.Success {
				matchIdx := args.LeaderPrevLogIndex
				// discard mismatched logs
				rf.log = rf.log[:rf.getSlicePosition(matchIdx)+1]
				// append leader's log
				rf.log = append(rf.log, args.LogEntries...)

				doPersist = true

				followerLastIndex, followerLastTerm := rf.lastLogInfo()
				rf.DPrintf(dLog, "log replicated, matchTerm: %d, matchIndex: %d, lastLogTerm: %d, lastLogIndex: %d", args.LeaderPrevLogTerm, matchIdx, followerLastTerm, followerLastIndex)
			}
		}
	}

	// do commit
	if reply.Success {
		if args.LeaderCommit > rf.commitIndex {
			myLastLogIndex := rf.getLastIndex()
			index := rf.commitIndex + 1
			if rf.commitIndex < rf.lastIncludedIndex {
				index = rf.lastIncludedIndex + 1
			}
			for ; index <= args.LeaderCommit && index <= myLastLogIndex; index++ {
				rf.commitIndex = index
				rf.DPrintf(dCommit, "commitIndex: {%d}\n", index)

				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					CommandIndex: index,
					Command:      rf.getLogEntryCommand(index),
				}
			}
		}
	}

	if doPersist {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.getLastIndex() + 1
	term = rf.currentTerm
	isLeader = rf.status == LEADER

	if !isLeader {
		return
	}

	rf.log = append(rf.log, LogEntry{term, command})
	rf.matchIndex[rf.me] = index
	rf.DPrintf(dClient, "new log append to leader [%d], newLogTerm: %d, newLogIndex: %d\n", rf.me, term, index)
	rf.persist()

	rf.triggerAE = true
	if rf.firstIndex == -1 {
		rf.firstIndex = index
	}

	// Your code here (2B).

	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// expect the caller to hold the lock
// generate and return a AppendEntriesArg for a specific server
func (rf *Raft) generateAEargs(server int) (args *AppendEntriesArgs, doInstallSnapshot bool) {
	firstLogIndexToSend := rf.nextIndex[server]
	prevLogIndex := firstLogIndexToSend - 1
	prevLogTerm := 0

	// the leader has already snapshoted logs before prevLogIndex
	// send IS instead of AE
	if prevLogIndex < rf.lastIncludedIndex {
		doInstallSnapshot = true
		return
	} else {
		prevLogTerm = rf.getLogEntryTerm(prevLogIndex)
	}

	// use a nil slice instead of adding a logsLength field
	var entries []LogEntry = nil
	if logsToSendLength := rf.getLastIndex() - prevLogIndex; logsToSendLength > 0 {
		entries = make([]LogEntry, logsToSendLength)
		copy(entries, rf.log[rf.getSlicePosition(firstLogIndexToSend):])
	}

	args = &AppendEntriesArgs{
		LeaderTerm:         rf.currentTerm,
		LeaderID:           rf.me,
		LeaderPrevLogIndex: prevLogIndex,
		LeaderPrevLogTerm:  prevLogTerm,
		LogEntries:         entries,
		LeaderCommit:       rf.commitIndex,
	}

	return
}

// make function that send RPC to one follower a named method
// so that once a follower return reply.Success = false, we could
// resend a RPC with new nextIndex immediately
func (rf *Raft) sendAEtoPeer(server int, args *AppendEntriesArgs, savedLeaderHeartbeatCount int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.status != LEADER {
			// rf.DPritf(dDrop, "no longer a leader, discard reply")
			return
		}

		if reply.Term > rf.currentTerm {
			rf.setFollower(reply.Term, NO_VOTE)
			rf.resetElectionTime()
			go rf.electionTicker(rf.currentTerm)

			rf.DPrintf(dTimer, "lagging behind other peers, heartbeat abort, convert to follower\n")

			rf.persist()
			return
		}

		rf.DPrintf(dLeader, "receive heartbeat RPC reply from [%d], replyTerm: %d, replySuccess: %v, replyXterm: %d, replyXIndex: %d, replyXLen: %d\n", server, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)

		if reply.Term < rf.currentTerm {
			rf.DPrintf(dDrop, "reply from previous term, discard reply\n")
			return
		}

		// follower do have LeaderPrevLogIndex and LeaderPrevLogTerm
		if reply.Success {

			// AE with logs, follower replicated or have received newer logs
			if args.LogEntries != nil {
				oldNextIndex := args.LeaderPrevLogIndex + 1
				newNextIndex := oldNextIndex + len(args.LogEntries)
				newMatchIndex := newNextIndex - 1

				if newMatchIndex > rf.matchIndex[server] && newNextIndex > rf.nextIndex[server] {
					couldCommit := (rf.firstIndex != -1 && oldNextIndex >= rf.firstIndex)
					rf.matchIndex[server] = newMatchIndex
					rf.nextIndex[server] = newNextIndex
					rf.DPrintf(dLeader, "log entries {%d->%d} replicated to [%d] success, for follower[%d], newMatchIndex: %d, newNextIndex: %d, COULD COMMIT: %v\n", oldNextIndex, newMatchIndex, server, server, newMatchIndex, newNextIndex, couldCommit)

					// do increment commit
					if couldCommit {
						rf.DPrintf(dLeader, "check if have some entries to commit, leaderCommitIndex: {%d}, leaderLastIndex: {%d}\n", rf.commitIndex, rf.getLastIndex())

						doApply := false
						savedCommitedIndex := rf.commitIndex
						if savedCommitedIndex < newMatchIndex {

							index := oldNextIndex
							if savedCommitedIndex >= index {
								index = savedCommitedIndex + 1
							}
							for ; index <= rf.getLastIndex(); index++ {
								matchCount := 0
								for j := range rf.peers {
									if index <= rf.matchIndex[j] {
										matchCount += 1
									}
									if matchCount*2 > len(rf.peers) {
										rf.commitIndex = index
										rf.DPrintf(dCommit, "leader commitIndex {%d}\n", index)
										doApply = true
										break
									}
								}

								// the i-th log entry is not replicated by most followers,
								// no need to check log entries after
								if index != rf.commitIndex {
									break
								}
							}
						}

						if doApply {
							for i := savedCommitedIndex + 1; i <= rf.commitIndex; i++ {
								rf.DPrintf(dClient, "send applyMsg to service, commandIndex: %d\n", i)
								rf.applyChan <- ApplyMsg{
									CommandValid: true,
									CommandIndex: i,
									Command:      rf.getLogEntryCommand(i),
								}
							}
						}
					}
				}
			}
		} else {
			// heartbeat success, but AE fail
			// the follower do not have prevLogIndex & prevLogTerm
			// in its log entries

			// finish faster match with XTerm & XIndex & XLen

			// heartbeat reply out-of-date, do not update nextIndex depend on it
			if rf.leaderHeartbeatCount > savedLeaderHeartbeatCount {
				return
			}

			// follower's log is too short
			// -----          follower's log
			//        -----   logEntries in AE
			if reply.XLen != -1 {
				rf.nextIndex[server] = reply.XLen + 1
			} else {

				// leader do not have XTerm
				// ------
				//     -----
				// (4, 4) vs (3, 4)
				if reply.XTerm > args.LeaderPrevLogTerm {
					rf.nextIndex[server] = reply.XIndex
				} else {

					// follower have logs before prevLogTerm : logs in XTerm
					// try the last log of follower reply conflicting term
					// ------
					//     -----
					// (2, 4) vs (3, 4)
					i := args.LeaderPrevLogIndex
					rf.nextIndex[server] = rf.lastIncludedIndex + 1
					for ; i > 0; i-- {
						if rf.getLogEntryTerm(i) <= reply.XTerm {
							rf.nextIndex[server] = i + 1
							break
						}
					}
				}
			}

			// resent a rpc with nextIndex decrease to this follower
			// immediately, instead of waiting for HEARTBEAT_INTERVAL
			newArgs, doInstallSnapshot := rf.generateAEargs(server)
			if doInstallSnapshot {
				go rf.sendIStoPeer(server)
				rf.DPrintf(dLog2, "prevLog mismatch, send IS to [%d]\n", server)
			} else {
				go rf.sendAEtoPeer(server, newArgs, savedLeaderHeartbeatCount)
				rf.DPrintf(dLog2, "prevLog mismatch, send AE with nextIndex: {%d}\n", rf.nextIndex[server])
			}
		}
	}
}

// expect the caller to hold the read lock
// launch a bunch of goroutines to
// send installSnapshot/appendEntries RPCs to all other peers
// and return immediately
// couldCommit only when triggered by Start()
// to avoid helping commit logs belongs to former leaders
func (rf *Raft) doHeartbeat() {
	rf.DPrintf(dLog, "begin sending heartbeat RPCs to followers\n")

	rf.leaderHeartbeatCount += 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send heartbeat RPCs to all other peers
			args, doInstallSnapshot := rf.generateAEargs(i)
			if doInstallSnapshot {
				go rf.sendIStoPeer(i)
				rf.DPrintf(dLeader, "send InstallSnapshot RPC to [%d]\n", i)
			} else {
				go rf.sendAEtoPeer(i, args, rf.leaderHeartbeatCount)
				rf.DPrintf(dLeader, "send heartbeat RPC to [%d], LeaderTerm: %d, LeaderPrevLogIndex: %d, LeaderPrevLogTerm: %d, LeaderCommit: %d", i, args.LeaderTerm, args.LeaderPrevLogIndex, args.LeaderPrevLogTerm, args.LeaderCommit)
				if args.LogEntries == nil {
					rf.DPrintf(dLeader, "without logs")
				} else {
					rf.DPrintf(dLeader, "with logs: {%d->%d}\n", args.LeaderPrevLogIndex+1, args.LeaderPrevLogIndex+len(args.LogEntries))
				}
			}
		}
	}

	// little test: check how much time a RPC takes
	// test result:
	// Success: 0ms, Fail: 1600+ms
}

// send and re-send RV RPCs to all peers in this election timeout
func (rf *Raft) doElection(savedCandidateTerm int) {

	voteReceived := make([]bool, len(rf.peers))
	voteReceived[rf.me] = true
	hasReply := make([]bool, len(rf.peers))
	hasReply[rf.me] = true
	electionDone := false

	for !rf.killed() {

		rf.mu.RLock()
		if rf.currentTerm != savedCandidateTerm || rf.status != CANDIDATE || electionDone {
			rf.DPrintf(dDrop, "election term_%d is no longer needed\n", savedCandidateTerm)
			electionDone = true
			rf.mu.RUnlock()
			return
		}

		savedLastLogIndex, savedLastLogTerm := rf.lastLogInfo()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && !hasReply[i] {

				// send request vote RPCs to all other peers
				args := &RequestVoteArgs{
					CandidateTerm:         savedCandidateTerm,
					CandidateID:           rf.me,
					CandidateLastLogIndex: savedLastLogIndex,
					CandidateLastLogTerm:  savedLastLogTerm,
				}
				rf.DPrintf(dVote, "send RequestVote RPC to [%d]\n", i)

				go func(server int) {
					reply := &RequestVoteReply{}

					// if !ok {
					// 	rf.DPritf(dVote, "RequestVote RPC to [%d] failed\n", server)
					// this would take 1600+ms
					// the election would be finished, just simply ignore it

					ok := rf.sendRequestVote(server, args, reply)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if electionDone {
							// rf.DPritf(dDrop, "election done, ignore reply from [%d]\n", server)
							return
						}

						hasReply[server] = true

						// handle reply in each goroutine
						// so that if a reply is receive within election term
						// the gouroutine would grab the lock and handle it
						// (change the candidate's state)
						// if a reply is receive after this election term
						// i.e. a new term is detected
						// the goroutine just simply return

						if rf.status != CANDIDATE {
							// no longer a candidate
							// rf.DPritf(dVote, "no longer a candidate, ignore reply\n")
							electionDone = true
							return
						}

						if savedCandidateTerm < rf.currentTerm {
							// this election is no longer needed
							// rf.DPritf(dVote, "new term detected, ignore reply\n")
							electionDone = true
							return
						}

						if reply.Term > rf.currentTerm {
							// lagging behind other peers
							// convert to follower and set to new term immediately
							rf.setFollower(reply.Term, NO_VOTE)
							rf.persist()
							rf.resetElectionTime()
							go rf.electionTicker(rf.currentTerm)
							rf.DPrintf(dDrop, "TERM lagging behind other peers, election abort, convert to follower of new term\n")

							electionDone = true
							return
						}

						rf.DPrintf(dVote, "receive RequestVote RPC reply from [%d], {%v}\n", server, reply)

						if reply.VoteGranted {
							voteReceived[server] = true

							voteCount := 0
							for i := range voteReceived {
								if voteReceived[i] {
									voteCount++
								}
							}

							rf.DPrintf(dVote, "vote granted from [%d], voteCount: %d/%d\n", server, voteCount, len(rf.peers))
							if voteCount*2 > len(rf.peers) {
								rf.DPrintf(dVote, "won election\n")
								rf.DPrintf(dTimer, "heartbeat ticker begin\n")

								rf.setLeader()
								go rf.heartbeatTicker(rf.currentTerm)

								electionDone = true
								return
							}
						} else {
							// no vote granted from valid leader of this term
							// convert to follower
							// and reset election ticker of this term
							if reply.Status == LEADER {
								rf.setFollower(rf.currentTerm, rf.me)
								rf.resetElectionTime()
								rf.DPrintf(dDrop, "come across with valid leader, election abort, convert to follower\n")

								electionDone = true
								return
							}

							// no vote granted from peer because of lagging logs
							rf.DPrintf(dLog, "LOG lagging behind other peers or have voted to other candidates, no vote granted from [%d]\n", server)
						}
					}
				}(i)
			}
		}

		replyCount := 0
		for i := range rf.peers {
			if hasReply[i] {
				replyCount += 1
			}
		}
		if replyCount == len(rf.peers) {
			electionDone = true
		}

		rf.mu.RUnlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// expect the caller to hold the lock
func (rf *Raft) setFollower(term, voteFor int) {
	rf.status = FOLLOWER
	rf.votedFor = voteFor
	rf.currentTerm = term
}

// expect the caller to hold the lock
func (rf *Raft) setCandidate() {
	rf.currentTerm++
	rf.status = CANDIDATE
	rf.votedFor = rf.me
}

// expect the caller to hold the lock
func (rf *Raft) setLeader() {
	rf.status = LEADER
	rf.heartbeatTime = time.Now()
	rf.firstIndex = -1
	rf.leaderHeartbeatCount = -1

	if rf.nextIndex == nil {
		rf.nextIndex = make([]int, len(rf.peers))
	}
	if rf.matchIndex == nil {
		rf.matchIndex = make([]int, len(rf.peers))
	}

	// set next log entry index sending to peers to
	// leader's last log index+1
	// at the very beginning of being a leader
	ni := rf.getLastIndex() + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = ni
	}

	// set highest match log entry index too peers
	// i.e. only need to care about log entries after this index
	// to 0 at the very beginning of being a leader
	// which stands for no matching log yet
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = ni - 1
}

// expect the caller to hold the read lock
func (rf *Raft) statusString() string {
	switch rf.status {
	case FOLLOWER:
		return "FOLL"
	case CANDIDATE:
		return "CAND"
	case LEADER:
		return "LEAD"
	default:
		return "UNKNOWN"
	}
}

// leader's heartbeat ticker
func (rf *Raft) heartbeatTicker(savedHeartbeatTerm int) {
	for !rf.killed() {

		rf.mu.Lock()

		if rf.currentTerm != savedHeartbeatTerm {
			rf.DPrintf(dTimer, "newer term detected heartbeat ticker term_%d abort\n", savedHeartbeatTerm)
			rf.mu.Unlock()
			return
		}

		if rf.status != LEADER {
			rf.DPrintf(dTimer, "no longer a leader, heartbeat ticker term_%d abort\n", savedHeartbeatTerm)
			rf.mu.Unlock()
			return
		}

		if rf.triggerAE {
			rf.triggerAE = false
			rf.doHeartbeat()
			rf.resetHeartbeatTime()
		} else if time.Now().After(rf.heartbeatTime) {
			rf.doHeartbeat()
			rf.resetHeartbeatTime()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// a elction would launch this goroutine once
// multiple electionTickers may run at the same time
// for a while
// but the older ones with older election term would
// return once they found out they are no longer needed
func (rf *Raft) electionTicker(savedElectionTerm int) {

	for !rf.killed() {

		// Your code here (2A)

		rf.mu.Lock()

		// this election ticker is no longer needed
		if rf.currentTerm != savedElectionTerm {
			rf.DPrintf(dTimer, "newer term detected election ticker term_%d abort\n", savedElectionTerm)
			rf.mu.Unlock()
			return
		}

		// the leader would not start election
		if rf.status == LEADER {
			rf.DPrintf(dTimer, "already a leader, election ticker term_%d abort\n", savedElectionTerm)
			rf.mu.Unlock()
			return
		}

		// election timeout
		if time.Now().After(rf.electionTime) {
			rf.DPrintf(dTimer, "election timeout, prepare to election\n")

			rf.setCandidate()
			rf.resetElectionTime()
			rf.persist()

			rf.DPrintf(dVote, "begin election\n")
			go rf.doElection(rf.currentTerm)

			// begin a election timer of the new term
			go rf.electionTicker(rf.currentTerm)

			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// expect the caller to hold the lock
func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime = time.Now().Add(HEARTHEAT_INTERVAL)
}

// expect the caller to hold the lock
func (rf *Raft) resetElectionTime() {
	rf.electionTimeout = randomTimeout()
	rf.electionTime = time.Now().Add(rf.electionTimeout)
	rf.DPrintf(dTimer, "election interval set to (%dms)\n", rf.electionTimeout.Milliseconds())
}

// generate a random time duration between 300ms and 500ms
func randomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

// expect the caller to hold the read lock
// debugging message formatter
func (rf *Raft) DPrintf(topic logTopic, format string, a ...interface{}) {
	if DebugVerbosity >= 1 {
		time := time.Since(DebugStart).Milliseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v [%d] %v, term_%v>: ", time, string(topic), rf.me, rf.statusString(), rf.currentTerm)
		format = prefix + format
		log.Printf(format, a...)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currentTerm:       0,
		votedFor:          NO_VOTE,
		log:               make([]LogEntry, 0),
		commitIndex:       0,
		lastApplied:       0,
		status:            FOLLOWER,
		applyChan:         applyCh,
		snapshot:          nil,
		lastIncludedTerm:  0,
		lastIncludedIndex: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.resetElectionTime()
	go rf.electionTicker(rf.currentTerm)
	rf.DPrintf(dTerm, "started\n")

	return rf
}
