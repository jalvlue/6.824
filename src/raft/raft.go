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

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	electionTimeout time.Duration
	electionTime    time.Time

	// volatile state on all servers
	commitIndex int
	lastApplied int
	status      byte
	applyChan   chan ApplyMsg

	// volatile state on leaders
	// for each server, index of the next log entry
	// to send to that server
	nextIndex []int

	// for each server, index of highest log entry
	// known to be replicated on server
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.status == LEADER

	return term, isleader
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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

	rf.DPritf(dVote, "receive RequestVoteRPC from [%d]\n", args.CandidateID)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.Status = rf.status

	if rf.currentTerm > args.Term {
		// in a higher term
		// no voet granted
		rf.DPritf(dVote, "in a higher term, no vote granted to [%d]\n", args.CandidateID)
		return
	}

	lastIndex, lastTerm := rf.lastLogInfo()

	if rf.currentTerm == args.Term &&
		(rf.votedFor == NO_VOTE || rf.votedFor == args.CandidateID) ||
		rf.currentTerm < args.Term &&
			(args.LastLogTerm > lastTerm ||
				(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		rf.DPritf(dVote, "vote granted to [%d]\n", args.CandidateID)
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		if rf.currentTerm < args.Term {
			// in a lower term
			// convert to follower to candidate of this term

			rf.DPritf(dVote, "in a lower term, convert to follow\n")
			rf.setFollower(args.Term, args.CandidateID)
			rf.resetElectionTime()
			// the older term election ticker would detect the new term
			// and return
			// new term election ticker would be launched
			go rf.electionTicker(rf.currentTerm)
			rf.DPritf(dVote, "election ticker begin (%dms)\n", rf.electionTimeout.Milliseconds())
		} else {
			// rf.currentTerm == args.Term
			// in the same term

			// not voted yet, and not newer than the cnadidate, vote granted
			rf.setFollower(args.Term, args.CandidateID)
			rf.resetElectionTime()
			rf.DPritf(dVote, "in the same term but no voted, vote granted to [%d]\n", args.CandidateID)
		}
	} else {
		rf.DPritf(dVote, "no vote granted to candidate [%d]", args.CandidateID)
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
	// leader's term when sending AE
	Term int

	// leader's id
	LeaderID int

	// highest log entry right before the new Entries sending
	// in this AE
	PrevLogIndex int

	// term of PrevLogIndex entry
	PrevLogTerm int

	// entries sending at this AE
	// empty for heartbeat
	Entries []LogEntry

	// index of leader's highest committed log entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Sussess bool
}

// append entries and heartbeat RPC
// for lab 2A only heartbeat is implemented
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPritf(dLog, "receive AppendEntries RPC from [%d]\n", args.LeaderID)

	reply.Term = rf.currentTerm
	reply.Sussess = false

	if rf.currentTerm > args.Term {
		// in a higher term
		rf.DPritf(dLog, "in a higher term, ignore heartbeat and notify leader [%d]\n", args.LeaderID)
		return
	} else if rf.currentTerm == args.Term {
		// in the same term
		if rf.status != FOLLOWER {
			rf.setFollower(args.Term, NO_VOTE)
		}
		rf.resetElectionTime()
	} else {
		// in a lower term
		// convert to follower

		rf.DPritf(dLog, "in a lower term, convert to follower\n")
		rf.setFollower(args.Term, NO_VOTE)
		rf.resetElectionTime()
		// the older term election ticker would detect the new term
		// and return
		// new term election ticker would be launched
		go rf.electionTicker(rf.currentTerm)
		rf.DPritf(dLog, "election ticker begin (%dms)\n", rf.electionTimeout.Milliseconds())
	}

	// heartbeat with log entries
	if len(args.Entries) != 0 {
		// not entries yet, just simply append leader's log
		if len(rf.log) == 0 {
			rf.log = append(rf.log, args.Entries...)
			reply.Sussess = true

			index, term := rf.lastLogInfo()
			rf.DPritf(dLog, "empty log, append leader's log, lastLogTerm: %d, lastLogIndex: %d\n", term, index)

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				rf.DPritf(dCommit, "commitIndex: %d", rf.commitIndex)
				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.commitIndex,
					Command:      rf.log[rf.commitIndex-1].Command,
				}
			}
		} else {
			// have some entries before
			for i, entry := range rf.log {
				// look for the first match entry
				index := i + 1
				if index == args.PrevLogIndex && entry.Term == args.PrevLogTerm {
					// discard mismatched logs
					rf.log = rf.log[:index]
					// append leader's log
					rf.log = append(rf.log, args.Entries...)
					followerLastIndex, followerLastTerm := rf.lastLogInfo()
					rf.DPritf(dLog, "matchTerm: %d, matchIndex: %d, lastLogTerm: %d, lastLogIndex: %d", entry.Term, index, followerLastTerm, followerLastIndex)
					// set success and commitIndex
					reply.Sussess = true
					if args.LeaderCommit > rf.commitIndex {
						rf.commitIndex = args.LeaderCommit
						rf.DPritf(dCommit, "commitIndex: %d", rf.commitIndex)
						rf.applyChan <- ApplyMsg{
							CommandValid: true,
							CommandIndex: rf.commitIndex,
							Command:      rf.log[rf.commitIndex-1].Command,
						}
					}
				}
			}
		}
	} else {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			rf.DPritf(dCommit, "commitIndex: %d", rf.commitIndex)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.commitIndex,
				Command:      rf.log[rf.commitIndex-1].Command,
			}
		}
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

	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.status == LEADER

	if !isLeader {
		return
	}

	rf.log = append(rf.log, LogEntry{term, command})
	rf.DPritf(dClient, "new log append to leader [%d], appendLogTerm: %d, appednLogIndex: %d\n", rf.me, term, len(rf.log))

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
// launch a bunch of goroutines to
// send heartbeat/appendEntries RPCs to all other peers
// and return immediately
func (rf *Raft) doHeartbeat() {
	rf.DPritf(dLog, "bgein sending heartbeat RPCs to followers\n")

	// start := time.Now()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send heartbeat RPCs to all other peers

			ni := rf.nextIndex[i]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
			entries := rf.log[prevLogIndex:]

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			if len(entries) == 0 {
				rf.DPritf(dTimer, "send heartbeat RPC to [%d]\n", i)
			} else {
				rf.DPritf(dTimer, "send AppendEntries RPC {%v} RPC to [%d]\n", entries, i)
			}

			go func(server int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					rf.DPritf(dTimer, "receive heartbeat RPC reply from [%d], {%v}\n", server, reply)

					if rf.status != LEADER {
						rf.DPritf(dLog, "no longer a leader, discard reply")
						return
					}

					if reply.Term > rf.currentTerm {
						rf.setFollower(reply.Term, NO_VOTE)
						rf.resetElectionTime()
						go rf.electionTicker(rf.currentTerm)

						rf.DPritf(dTimer, "lagging behind other peers, heartbeat abort, convert to follower\n")
						rf.DPritf(dTimer, "election ticker begin (%dms)\n", rf.electionTimeout.Milliseconds())
						return
					}

					rf.DPritf(dLog, "heartbeatRPC to [%d] success\n", server)

					// with log entry
					if len(args.Entries) != 0 {
						// replicated success
						if reply.Sussess {
							rf.matchIndex[server] = ni + len(entries) - 1
							rf.nextIndex[server] = ni + len(entries)
							rf.DPritf(dLeader, "log entries {%d->%d} replicated to [%d] success\n", ni, ni+len(entries)-1, server)
							rf.DPritf(dLeader, "for follower[%d], matchIndex: %d, nextIndex: %d", server, rf.matchIndex[server], rf.nextIndex[server])

							for index := rf.commitIndex + 1; index < len(rf.log)+1; index++ {
								matchCount := 0
								for j := range rf.peers {
									if index <= rf.matchIndex[j] {
										matchCount++
									}
									if matchCount*2 > len(rf.peers) {
										rf.commitIndex = index
										rf.DPritf(dCommit, "leader commitIndex {%d}", index)
										rf.applyChan <- ApplyMsg{
											CommandValid: true,
											CommandIndex: index,
											Command:      rf.log[index-1].Command,
										}
									}
								}
							}
						} else {
							// heartbeat success, but AE fail
							// decrement the nextIndex to this server
							// and wait for next AE try
							rf.nextIndex[server]--
							rf.DPritf(dLog, "prevLogIndex not match, decrement nextIndex for [%d]\n", server)
						}
					}
				}
				// } else {
				// rf.DPritf(dTimer, "heartbeat RPC to [%d] failed", server)
				// this would take 1600+ms
				// just simply ignore it
				// return
			}(i)
		}
	}

	// little test: check how many times a RPC takes
	// Success: 0ms, Fail: 1600+ms
	// epalseMs := time.Since(start).Nanoseconds() / 1e6
	// rf.debugLogger.Printf(" %v, term_%v>: heartbeat RPCs done, epalse: %vms\n", statusString(leaderStatus), leaderTerm, epalseMs)
}

// expect the caller to hold the lock
// launch a bunch of goroutines and return immediately
func (rf *Raft) startElection(savedCandidateTerm int) {
	rf.DPritf(dVote, "begin election\n")

	voteReceived := 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send request vote RPCs to all other peers
			savedLastLogIndex, savedLastLogTerm := rf.lastLogInfo()
			args := &RequestVoteArgs{
				Term:         savedCandidateTerm,
				CandidateID:  rf.me,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			rf.DPritf(dVote, "send RequestVote RPC to [%d]\n", i)

			go func(server int) {
				reply := &RequestVoteReply{}

				ok := rf.sendRequestVote(server, args, reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					rf.DPritf(dVote, "receive RequestVote RPC reply from [%d], {%v}\n", server, reply)

					// handle reply in each goroutine
					// so that if a reply is receive within timeout
					// the gouroutine would grab the lock and handle it
					// (change the candidate's state)
					// if a reply is receive after timeout
					// i.e. a new term is detected
					// the goroutine just simply return

					if rf.status != CANDIDATE {
						// no longer a candidate
						rf.DPritf(dVote, "no longer a candidate, ignore reply\n")
						return
					}

					if savedCandidateTerm < rf.currentTerm {
						// this election is no longer needed
						rf.DPritf(dVote, "new term detected, election abort\n")
						return
					}

					if reply.Term > savedCandidateTerm {
						// lagging behind other peers
						// convert to follower and set to new term immediately
						rf.setFollower(reply.Term, NO_VOTE)
						rf.resetElectionTime()
						go rf.electionTicker(rf.currentTerm)
						rf.DPritf(dVote, "lagging behind other peers, election abort, convert to follower\n")
						rf.DPritf(dTimer, "election ticker begin (%dms)\n", rf.electionTimeout.Milliseconds())
						return
					}

					if reply.VoteGranted {
						voteReceived++
						rf.DPritf(dVote, "vote granted from [%d], voteCount: %d/%d\n", server, voteReceived, len(rf.peers))

						if voteReceived*2 > len(rf.peers) {
							rf.DPritf(dVote, "won election\n")
							rf.DPritf(dTimer, "heartbeat ticker begin\n")

							rf.setLeader()
							go rf.heartbeatTicker(rf.currentTerm)

							return
						}

					} else {
						rf.DPritf(dVote, "RequestVote RPC to [%d] failed\n", server)
						// TODO: record and resend

						// this would take 1600+ms
						// just simply ignore it
						return
					}
				}
			}(i)
		}
	}

	go rf.electionTicker(rf.currentTerm)
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

	// set next log entry index sending to peers to
	// leader's last log index+1 at the very beginning of being a leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
	}

	// set highest match log entry index too peers
	// i.e. only need to care about log entries after this index
	// to 0 at the very beginning of being a leader
	// which stands for no matching log yet
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log)
}

// expect the caller to hold the lock
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

// expect the caller to hold the lock
// get index, term of last log
func (rf *Raft) lastLogInfo() (lastLogIndex, lastLogTerm int) {
	if len(rf.log) != 0 {
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-1].Term
		return
	}
	return 0, 0
}

func (rf *Raft) heartbeatTicker(thisHeartbeatTerm int) {
	for !rf.killed() {

		rf.mu.Lock()

		if rf.currentTerm != thisHeartbeatTerm {
			rf.DPritf(dTimer, "newer term detected heartbeat ticker term_%d abort\n", thisHeartbeatTerm)
			rf.mu.Unlock()
			return
		}

		if rf.status != LEADER {
			rf.DPritf(dTimer, "no longer a leader, heartbeat ticker term_%d abort\n", thisHeartbeatTerm)
			rf.mu.Unlock()
			return
		} else {
			rf.doHeartbeat()
			rf.mu.Unlock()
		}

		time.Sleep(HEARTHEAT_INTERVAL)
	}
}

// a elction would launch this goroutine once
// multiple electionTickers may run at the same time
// for a whild
// but the older ones with older election term would
// return once they found out they are no longer needed
func (rf *Raft) electionTicker(thisElectionTerm int) {

	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()

		// this election ticker is no longer needed
		if rf.currentTerm != thisElectionTerm {
			rf.DPritf(dTimer, "newer term detected election ticker term_%d abort\n", thisElectionTerm)
			rf.mu.Unlock()
			return
		}

		// the leader would not start election
		if rf.status == LEADER {
			rf.DPritf(dTimer, "already a leader, election ticker term_%d abort\n", thisElectionTerm)
			rf.mu.Unlock()
			return
		}

		// election timeout
		if time.Now().After(rf.electionTime) {
			rf.DPritf(dTimer, "election timeout, prepare to election\n")

			rf.setCandidate()
			rf.resetElectionTime()
			rf.startElection(rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// expect the caller to hold the lock
func (rf *Raft) resetElectionTime() {
	rf.electionTimeout = randomTimeout()
	rf.electionTime = time.Now().Add(rf.electionTimeout)
}

// generate a random timeout between 300ms and 450ms
func randomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

// expect the caller to hold the lock
func (rf *Raft) DPritf(topic logTopic, format string, a ...interface{}) {
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.setFollower(0, NO_VOTE)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTime()
	go rf.electionTicker(rf.currentTerm)
	rf.DPritf(dTerm, "started\n")
	rf.DPritf(dTimer, "election ticker begin(%dms)\n", rf.electionTimeout.Milliseconds())

	return rf
}
