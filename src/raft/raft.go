package raft

//
// this is an outline of the API that tmp must expose to
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
	"encoding/json"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ELECTION_TICKER  = time.Duration(10) * time.Millisecond
	HEARTBEAT_TICKER = time.Duration(10) * time.Millisecond
)

const (
	MIN_ELECTION_TIMEOUT = 350
	MAX_ELECTION_TIMEOUT = 1500
	HEARTBEAT_INTERVAL   = 110 // 120 MS
)

const (
	FOLLOWER  int32 = 1
	CANDIDATE int32 = 2
	LEADER    int32 = 3
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
//
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

// A Raft log entry
type Entry struct {
	Command interface{}
	Term    int // when each entry was received by leader(first index is 1)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int      // last term server has seen
	votedFor    int      // candidateId that received vote in current term(or -1 if none)
	log         []*Entry // log entries (1,2,3...)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (0,1,2..)
	lastApplied int // index of highest log entry applied to state machine (0,1,2..)

	// Volatile state on leaders. Reinitialized after election.
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry to known to be replicated on server(0,1,2...)

	// Leader Election
	leaderId int
	state    int32

	electionCh      chan bool
	electionCond    *sync.Cond
	electionTimeout time.Duration
	lastHeardTime   time.Time

	heartbeatCh       chan bool
	heartbeatCond     *sync.Cond
	heartbeatInterval time.Duration
	lastIssueTime     time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//argStr, _ := json.Marshal(args)
	//replyStr, _ := json.Marshal(reply)
	//DPrintf("[Receive AppendEntries] server:%d args:%s reply:%s", rf.me, argStr, replyStr)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	isSmallerTerm := args.Term < rf.currentTerm
	rf.mu.Unlock()

	if isSmallerTerm {
		DPrintf("--->argsTerm:%d currentTerm:%d", args.Term, rf.currentTerm)
		reply.Success = false
		return
	}

	rf.resetElectionTimeout()
	rf.mu.Lock()
	rf.lastIssueTime = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	reply.Success = true
	rf.votedFor = -1
	rf.leaderId = args.LeaderId
	rf.state = FOLLOWER
	rf.electionCond.Signal()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's last log entry term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	argStr, _ := json.Marshal(args)
	DPrintf("[Receive RequestVote] server:%d from:%d args:%s", rf.me, args.CandidateId, string(argStr))
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	isSmallerTerm := args.Term < rf.currentTerm
	rf.mu.Unlock()
	if isSmallerTerm {
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.electionCond.Signal()
		rf.mu.Unlock()
		rf.resetElectionTimeout()
	}

	rf.mu.Lock()
	canVote := rf.state == FOLLOWER && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	lastLogTerm := rf.log[len(rf.log)-1].Term
	isUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)-1)
	DPrintf("server:%d canVote:%t isUpToDate:%t", rf.me, canVote, isUpToDate)
	rf.mu.Unlock()
	if canVote && isUpToDate {
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.mu.Unlock()
		rf.resetElectionTimeout()
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimeout() {
	//DPrintf("[Reset Election Timeout] %s", where)
	rf.mu.Lock()
	rf.lastHeardTime = time.Now()
	rf.electionTimeout = time.Duration(MIN_ELECTION_TIMEOUT+rand.Int31n(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)) * time.Millisecond
	rf.mu.Unlock()
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[StartElection] server:%d state:%d", rf.me, rf.state)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderId = -1
	rf.state = CANDIDATE
	rf.electionCond.Signal()

	var voteNum int32 = 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	rf.mu.Unlock()
	rf.resetElectionTimeout()

	for i, _ := range rf.peers {
		go func(serverIdx int) {
			if serverIdx != rf.me {
				reply := RequestVoteReply{}
				rf.sendRequestVote(serverIdx, &args, &reply)
				rf.mu.Lock()
				isCandidate := rf.state == CANDIDATE
				rf.mu.Unlock()
				if reply.VoteGranted && isCandidate {
					if atomic.AddInt32(&voteNum, 1) > int32(len(rf.peers)/2) && isCandidate {
						rf.mu.Lock()
						rf.state = LEADER
						DPrintf("[===> Become Leader] leader:%d", rf.me)
						rf.heartbeatCond.Signal()
						rf.leaderId = rf.me
						rf.votedFor = -1
						rf.mu.Unlock()
						rf.heartbeatCh <- true
					}

				}

				rf.mu.Lock()
				isInvalidCandidate := !reply.VoteGranted && reply.Term > rf.currentTerm
				rf.mu.Unlock()
				if isInvalidCandidate {
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.electionCond.Signal()
					rf.leaderId = -1
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
				}
			}
		}(i)
	}

}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, rf.commitIndex}
	rf.lastIssueTime = time.Now()
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(serverIdx int) {
				reply := AppendEntriesReply{}
				//DPrintf("[SendHeartbeat-Start-%d] from:%d to:%d term:%d", time.Now().UnixNano()/1000000, rf.me, serverIdx, rf.currentTerm)
				rf.sendAppendEntries(serverIdx, &args, &reply)
				rf.mu.Lock()
				isInvalidLeader := rf.state == LEADER && reply.Term > rf.currentTerm
				rf.mu.Unlock()
				if isInvalidLeader {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.leaderId = -1
					rf.state = FOLLOWER

					rf.lastHeardTime = time.Now()
					rf.electionCond.Signal()
					rf.mu.Unlock()
					rf.resetElectionTimeout()
				}
				//DPrintf("[SendHeartbeat-Reply-%d] from:%d to:%d term:%d success:%t", time.Now().UnixNano()/1000000, rf.me, serverIdx, reply.Term, reply.Success)
			}(i)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) eventLoop() {
	for rf.killed() == false {
		select {
		case <-rf.electionCh:
			rf.startElection()
		case <-rf.heartbeatCh:
			rf.sendHeartbeat()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano() * int64(me))

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.log = []*Entry{}
	rf.log = append(rf.log, &Entry{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.leaderId = -1
	rf.state = FOLLOWER

	rf.electionCh = make(chan bool)
	rf.electionCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimeout()

	rf.heartbeatCh = make(chan bool)
	rf.heartbeatCond = sync.NewCond(&rf.mu)
	rf.heartbeatInterval = time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond
	rf.lastIssueTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTimeoutTicker()
	go rf.heartbeatTicker()
	go rf.eventLoop()

	return rf
}

func (rf *Raft) electionTimeoutTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.electionCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			isElectionTimeout := time.Now().After(rf.lastHeardTime.Add(rf.electionTimeout))
			rf.mu.Unlock()
			if isElectionTimeout {
				DPrintf("[Election Timeout] server:%d state:%d timeout:%d", rf.me, rf.state, rf.electionTimeout/1000000)
				rf.electionCh <- true
			}
		}
		time.Sleep(ELECTION_TICKER)
	}
}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); !isLeader {
			rf.mu.Lock()
			rf.heartbeatCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			isHeartbeatTrigger := time.Now().After(rf.lastIssueTime.Add(rf.heartbeatInterval))
			rf.mu.Unlock()
			if isHeartbeatTrigger {
				rf.heartbeatCh <- true
			}
		}
		time.Sleep(HEARTBEAT_TICKER)
	}
}
