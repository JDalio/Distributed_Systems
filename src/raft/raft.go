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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

type Role int

const (
	fOLLOWER Role = iota
	CANDIDATE
	LEADER
)

const MIN_ELECTION_TIMEOUT = 200 //rand in [200,300) millisecond
const HEARTBEAT_DURATION = 100

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor    interface{}
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//Custom
	role      Role
	timeout   *time.Timer
	heartbeat *time.Ticker

	voteChan          chan *RequestVoteReply
	voteNum           int
	appendEntriesChan chan *AppendEntriesArgs
	toFollowerChan    chan int
}

func (rf *Raft) beFollower() {
	rf.role = fOLLOWER
	rf.ResetTimer()
	rf.votedFor = nil
	DPrintf("Follower [%d] init, Term: %d, Now: %d", rf.me, rf.currentTerm, time.Now().UnixNano())
	for {
		select {
		case <-rf.appendEntriesChan:
			rf.ResetTimer()
		case <-rf.timeout.C:
			DPrintf("Follower [%d] timeout, Now: %d", rf.me, time.Now().UnixNano())
			go rf.beCandidate()
			return
		}
	}
}

func (rf *Raft) beCandidate() {
	DPrintf("Candidate [%d] init, Term: %d, Votenum: %d", rf.me, rf.currentTerm, rf.voteNum)
	rf.role = CANDIDATE
	rf.voteChan = make(chan *RequestVoteReply)
	rf.startElection()
	for {
		select {
		case reply := <-rf.voteChan:
			if reply.VoteGranted {
				rf.voteNum++
			}
			if rf.voteNum > len(rf.peers)/2 {
				go rf.beLeader()
				return
			}
		case <-rf.appendEntriesChan:
			go rf.beFollower()
			return
		case <-rf.timeout.C:
			rf.startElection()
			DPrintf("Candidate [%d] timeout, Term: %d", rf.me, rf.currentTerm)
		case <-rf.toFollowerChan:
			go rf.beFollower()
			return
		}
	}

}

func (rf *Raft) beLeader() {
	rf.role = LEADER
	DPrintf("Leader [%d] init, Term: %d", rf.me, rf.currentTerm)
	rf.sendHeartbeat()
	for {
		select {
		case <-rf.heartbeat.C:
			rf.sendHeartbeat()
		case <-rf.toFollowerChan:
			go rf.beFollower()
			return
		}
	}
}

func (rf *Raft) ResetTimer() {
	if rf.timeout != nil {
		rf.timeout.Stop()
		rf.timeout = nil
	}

	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	d := time.Millisecond * time.Duration(MIN_ELECTION_TIMEOUT+rand.Intn(100))
	rf.timeout = time.NewTimer(d)
	rf.timeout.Reset(d)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == LEADER
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.ResetTimer()
	// Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		DPrintf("-RequestVote1- Role: %d SelfId: %d currentTerm: %d argsTerm: %d argsId: %d", rf.role, rf.me, rf.currentTerm, args.Term, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		rf.mu.Unlock()
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		DPrintf("-RequestVote2- Role: %d SelfId: %d currentTerm: %d argsTerm: %d argsId: %d", rf.role, rf.me, rf.currentTerm, args.Term, args.CandidateId)
		if rf.role == LEADER || rf.role == CANDIDATE {
			rf.toFollowerChan <- args.Term
		}
	}
	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log,
	// grant vote (§5.2, §5.4)
	//  && (rf.commitIndex >= args.LastLogIndex && rf.log[rf.commitIndex].Term >= args.LastLogTerm)
	if rf.votedFor == nil || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.appendEntriesChan <- args
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.role != fOLLOWER {
			rf.toFollowerChan <- args.Term
		}
	} else if args.Term == rf.currentTerm {
		rf.votedFor = args.LeaderId
	}

	//if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
	//	reply.Success = false
	//	return
	//}
	//TODO
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat() {
	DPrintf("Leader [%d] send heartbeat, term: %d", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := new(AppendEntriesArgs)
			args.Term = rf.currentTerm
			args.LeaderId = rf.me

			reply := new(AppendEntriesReply)
			go rf.sendAppendEntries(i, args, reply)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("-sendAppendEntries- Leader:%d Role:%d To:%d", rf.me, rf.role, server)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.toFollowerChan <- reply.Term
		}
	}
	return ok
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
	if ok {
		if rf.role == CANDIDATE {
			rf.voteChan <- reply
			if reply.VoteGranted {
				DPrintf("-sendRequestVote- Candidate %d Receive vote from %d", rf.me, server)
			} else {
				DPrintf("-sendRequestVote- Candidate %d Receive veto from %d", rf.me, server)

			}
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.toFollowerChan <- reply.Term
		}
	}
	return ok
}

func (rf *Raft) startElection() {
	rf.voteNum = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetTimer()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := new(RequestVoteArgs)
			reply := new(RequestVoteReply)
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			go rf.sendRequestVote(i, args, reply)
		}
	}
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize timer
	rf.voteChan = make(chan *RequestVoteReply)
	rf.appendEntriesChan = make(chan *AppendEntriesArgs)
	rf.toFollowerChan = make(chan int)

	d := time.Millisecond * time.Duration(HEARTBEAT_DURATION)
	rf.heartbeat = time.NewTicker(d)

	go rf.beFollower()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
