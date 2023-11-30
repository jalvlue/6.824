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
	"os"
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

func statusString(status byte) string {
	switch status {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

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

	// lab 2A
	debugLogger *log.Logger

	// persistent state on all servers
	currentTerm     int
	votedFor        int
	log             []LogEntry
	electionTimeout time.Duration
	electionTime    time.Time

	// volatile state on all servers
	commitIndex     int
	lastApplied     int
	replicatedIndex int
	status          byte

	// volatile state on leaders
	nextIndex  []int
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

	rf.debugLogger.Printf(" %v, term_%v>: receive RequestVote RPC from %d\n", statusString(rf.status), rf.currentTerm, args.CandidateID)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.Status = rf.status

	// TODO: handle vote state
	if rf.currentTerm > args.Term {
		// in a higher term
		// no voet granted
		rf.debugLogger.Printf(" %v, term_%v>: in a higher term, no vote granted to %d\n", statusString(rf.status), rf.currentTerm, args.CandidateID)
		return
	} else if rf.currentTerm == args.Term {
		// in the same term
		if rf.votedFor == NO_VOTE {
			// not voted yet, vote to the candidate
			// candidates and leaders would vote to themselves
			// so it must be a follower
			rf.setFollower(args.Term, args.CandidateID)
			rf.resetElectionTime()
			reply.VoteGranted = true
			rf.debugLogger.Printf(" %v, term_%v>: in the same term but no voted, vote granted to %d\n", statusString(rf.status), rf.currentTerm, args.CandidateID)
		} else {
			// already voted, no vote granted
			rf.debugLogger.Printf(" %v, term_%v>: already voted in this term, no vote granted to %d\n", statusString(rf.status), rf.currentTerm, args.CandidateID)
			return
		}
	} else {
		// in a lower term
		// vote to the candidate
		// convert to follower
		rf.setFollower(args.Term, args.CandidateID)
		rf.resetElectionTime()
		reply.VoteGranted = true
		rf.debugLogger.Printf(" %v, term_%v>: in a lower term, vote granted to %d\n", statusString(rf.status), rf.currentTerm, args.CandidateID)
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
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
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

	rf.debugLogger.Printf(" %v, term_%v>: receive AppendEntries RPC from %d\n", statusString(rf.status), rf.currentTerm, args.LeaderID)

	reply.Term = rf.currentTerm
	reply.Sussess = false

	// only care about heartbeat in 2A
	// TODO: implement append entries
	if rf.currentTerm > args.Term {
		// in a higher term
		rf.debugLogger.Printf(" %v, term_%v>: in a higher term, ignore heartbeat and notify leader %d\n", statusString(rf.status), rf.currentTerm, args.LeaderID)
		return
	} else if rf.currentTerm == args.Term {
		// in the same term
		rf.setFollower(args.Term, rf.votedFor)
		rf.resetElectionTime()
	} else {
		// in a lower term
		// convert to follower

		rf.debugLogger.Printf(" %v, term_%v>: in a lower term, convert to follower\n", statusString(rf.status), rf.currentTerm)
		rf.setFollower(args.Term, NO_VOTE)
		rf.resetElectionTime()
		rf.debugLogger.Printf(" %v, term_%v>: election ticker begin\n", statusString(rf.status), rf.currentTerm)
	}
	reply.Sussess = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// send heartbeat/appendEntries RPCs to all other peers
func (rf *Raft) doHeartbeat(leaderTerm int, leaderStatus byte) {
	hbTerm := leaderTerm

	// start := time.Now()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send heartbeat RPCs to all other peers
			go func(server int) {

				rf.debugLogger.Printf(" %v, term_%v>: send heartbeat RPC to %d\n", statusString(leaderStatus), leaderTerm, server)

				args := &AppendEntriesArgs{
					Term:     leaderTerm,
					LeaderID: rf.me,
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if !ok {
					// rf.debugLogger.Printf(" %v, term_%v>: heartbeat RPC to %d failed", statusString(leaderStatus), leaderTerm, server)

					// this would take 1600+ms
					// just simply ignore it
					return
				}

				rf.mu.Lock()

				if reply.Term > hbTerm {
					hbTerm = reply.Term
				}

				rf.mu.Unlock()
			}(i)
		}
	}

	// little test: check how many times a RPC takes
	// Success: 0ms, Fail: 1600+ms
	// epalseMs := time.Since(start).Nanoseconds() / 1e6
	// rf.debugLogger.Printf(" %v, term_%v>: heartbeat RPCs done, epalse: %vms\n", statusString(leaderStatus), leaderTerm, epalseMs)

	// sleep for a while(50ms) waiting for heartbeat reply
	time.Sleep(HEARTHEAT_INTERVAL / 2)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < hbTerm {
		// lagging behind other peers
		// convert to follower and set to new term immediately
		rf.setFollower(hbTerm, NO_VOTE)
		rf.resetElectionTime()
		go rf.electionTicker()
		rf.debugLogger.Printf(" %v, term_%v>: lagging behind other peers, heartbeat abort, convert to follower\n", statusString(rf.status), rf.currentTerm)
		rf.debugLogger.Printf(" %v, term_%v>: election ticker begin\n", statusString(rf.status), rf.currentTerm)
		return
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) startElection(candidateTerm int, candidateStatus byte) {
	rf.debugLogger.Printf(" %v, term_%v>: begin election\n", statusString(candidateStatus), candidateTerm)

	voteMu := sync.Mutex{}
	voteCount := 1
	voteValidLeader := false
	voteTerm := candidateTerm
	// TODO: resend vote request if no response before timeout
	// maybe use a slice of bool to record vote result

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send request vote RPCs to all other peers

			go func(server int) {
				rf.debugLogger.Printf(" %v, term_%v>: send RequestVote RPC to %d\n", statusString(candidateStatus), candidateTerm, server)

				args := &RequestVoteArgs{
					Term:        candidateTerm,
					CandidateID: rf.me,
				}
				reply := &RequestVoteReply{}

				ok := rf.sendRequestVote(server, args, reply)
				if !ok {
					// rf.debugLogger.Printf(" %v, term_%v>: RequestVote RPC to %d failed", statusString(candidateStatus), candidateTerm, server)
					// TODO: record and resend

					// this would take 1600+ms
					// just simply ignore it
					return
				}

				voteMu.Lock()

				// vote granted
				if reply.VoteGranted {
					voteCount++
					rf.debugLogger.Printf(" %v, term_%v>: vote granted from %d, voteCount: %d/%d\n", statusString(candidateStatus), candidateTerm, server, voteCount, len(rf.peers))
				} else {
					// no vote granted
					rf.debugLogger.Printf(" %v, term_%v>: no vote granted from %d, voteCount: %d/%d\n", statusString(candidateStatus), candidateTerm, server, voteCount, len(rf.peers))

					// came across a valid leader
					if reply.Status == LEADER {
						voteValidLeader = true
						rf.debugLogger.Printf(" %v, term_%v>: came across valid leader %d\n", statusString(candidateStatus), candidateTerm, server)
					}

					// lagging behind other peers
					if reply.Term > voteTerm {
						voteTerm = reply.Term
						rf.debugLogger.Printf(" %v, term_%v>: lagging behind %d\n", statusString(candidateStatus), candidateTerm, server)
					}
				}

				voteMu.Unlock()
			}(i)
		}
	}

	// collect votes and check if won election
	// only wait for around 250ms
	// since the election timeout is 300-450ms
	// so the election would be done before the next election timeout
	// RPCs sent after this time would be ignored
	counter := 0
	for {
		time.Sleep(50 * time.Millisecond)

		voteMu.Lock()
		if voteCount*2 > len(rf.peers) || voteValidLeader || voteTerm > candidateTerm {
			voteMu.Unlock()
			break
		}

		counter++
		if counter > 5 {
			voteMu.Unlock()
			break
		}

		voteMu.Unlock()
	}

	// kill older election ticker
	// set a new election or heartbeat ticker
	voteMu.Lock()
	defer voteMu.Unlock()

	rf.debugLogger.Printf(" %v, term_%v>: election done, voteCount: %d/%d\n", statusString(candidateStatus), candidateTerm, voteCount, len(rf.peers))

	// TODO: done ticker leads to a deadlock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// lagging behind other peers or came across with valid leader
	// convert to follower and set to new term immediately
	if rf.currentTerm < voteTerm || voteValidLeader {
		rf.setFollower(voteTerm, NO_VOTE)
		rf.resetElectionTime()
		rf.debugLogger.Printf(" %v, term_%v>: election abort due to lagging or valid leader, convert to follower\n", statusString(rf.status), rf.currentTerm)
		return
	} else {

		// return election result
		if voteCount*2 > len(rf.peers) {
			// won election

			rf.setLeader()
			rf.debugLogger.Printf(" %v, term_%v>: won election\n", statusString(rf.status), rf.currentTerm)
			rf.debugLogger.Printf(" %v, term_%v>: heartbeat ticker begin\n", statusString(rf.status), rf.currentTerm)
		} else {
			// lost election
			rf.debugLogger.Printf(" %v, term_%v>: lost election, wait for next election try\n", statusString(rf.status), rf.currentTerm)
		}
	}
}

func (rf *Raft) setFollower(term, voteFor int) {
	// the caller would hold the lock

	rf.status = FOLLOWER
	rf.votedFor = voteFor
	rf.currentTerm = term
	// rf.doneChan <- struct{}{}
	// rf.resetElectionTime()

	// go rf.electionTicker()
}

func (rf *Raft) setCandidate() {
	// the caller would hold the lock

	rf.currentTerm++
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	// rf.doneChan <- struct{}{}
	// rf.resetElectionTime()

	// go rf.electionTicker()
}

func (rf *Raft) setLeader() {
	// the caller would hold the lock

	rf.status = LEADER
	// rf.doneChan <- struct{}{}
	// rf.resetElectionTime()

	// go rf.heartbeatTicker()
}

// long running goroutine
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {

		time.Sleep(HEARTHEAT_INTERVAL)
		rf.mu.Lock()

		if rf.status != LEADER {
			rf.mu.Unlock()
			time.Sleep(HEARTHEAT_INTERVAL)
		} else {

			rf.debugLogger.Printf(" %v, term_%v>: send heartbeat RPC to followers\n", statusString(rf.status), rf.currentTerm)

			go rf.doHeartbeat(rf.currentTerm, rf.status)

			rf.mu.Unlock()
		}
	}
}

// long running goroutine
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		// time.Sleep(HEARTHEAT_INTERVAL)
		time.Sleep(HEARTHEAT_INTERVAL)
		rf.mu.Lock()

		// the leader would not start election
		if rf.status == LEADER {
			rf.mu.Unlock()
			time.Sleep(HEARTHEAT_INTERVAL)
		} else {
			if time.Now().After(rf.electionTime) {
				rf.debugLogger.Printf(" %v, term_%v>: election timeout, prepare to election\n", statusString(rf.status), rf.currentTerm)

				rf.setCandidate()
				rf.resetElectionTime()
				go rf.startElection(rf.currentTerm, rf.status)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) resetElectionTime() {
	// the caller would hold the lock

	rf.electionTimeout = randomTimeout()
	rf.electionTime = time.Now().Add(rf.electionTimeout)
}

// generate a random timeout between 300ms and 450ms
func randomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
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

	// Your initialization code here (2A, 2B, 2C).
	rf.status = FOLLOWER
	rf.votedFor = NO_VOTE
	rf.currentTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// debug log
	logFile, err := os.OpenFile("log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	rf.debugLogger = log.New(logFile, fmt.Sprintf("[%d]", rf.me), 0)
	// rf.debugLogger.SetOutput(os.Stdout)

	// start long run ticker goroutine to handle election and heartbeat
	rf.resetElectionTime()
	go rf.electionTicker()
	go rf.heartbeatTicker()
	rf.debugLogger.Printf(" %v, term_%v>: started\n", statusString(rf.status), rf.currentTerm)
	rf.debugLogger.Printf(" %v, term_%v>: election ticker begin\n", statusString(rf.status), rf.currentTerm)

	return rf
}
