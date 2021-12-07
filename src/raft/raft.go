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
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ElectionTimeoutFactor = 4
)

const (
	Stopped   = -1
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	log       *Log

	// Your data here (2A, 2B, 2C).
	state       int
	leader      int
	currentTerm int
	votedFor    int

	electionTimeout   time.Duration
	heartbeatInterval time.Duration

	c chan *ev
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.leader == rf.me
}
func (rf *Raft) setLeader(leader int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = leader
}
func (rf *Raft) State() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state
}
func (rf *Raft) SetState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}
func (rf *Raft) MemberCount() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.peers)
}
func (rf *Raft) QuorumSize() int {
	return rf.MemberCount()/2 + 1
}
func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionTimeout
}
func (rf *Raft) SetElectionTimeout(timeout time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = timeout
}
func (rf *Raft) HeartbeatInterval() time.Duration {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.heartbeatInterval
}
func (rf *Raft) SetHeartbeatInterval(interval time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatInterval = interval
}
func (rf *Raft) incrCurrentTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
}
func (rf *Raft) CurrentTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}
func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}
func (rf *Raft) updateCurrentTerm(term int, leaderId int) {
	state := rf.State()
	if state == Leader {
		//TODO stop heartbeat before step-down
	}
	if state != Follower {
		rf.SetState(Follower)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.leader = leaderId
	rf.votedFor = -1
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

func (rf *Raft) processVoteRequest(args *RequestVoteRequest, reply *RequestVoteReply) bool {
	reply.Term = rf.currentTerm
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return false
	}

	if args.Term > rf.currentTerm {
		rf.updateCurrentTerm(args.Term, -1)
	}

	lastLogIndex, lastLogTerm := rf.log.lastInfo()
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	isUpToDate := args.Term > lastLogTerm || (args.Term == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if canVote && isUpToDate {
		reply.VoteGranted = true
		return true
	}

	reply.VoteGranted = false

	return false
}
func (rf *Raft) processVoteReply(reply *RequestVoteReply) (isVoted bool) {
	if reply.Term > rf.currentTerm {
		rf.updateCurrentTerm(reply.Term, -1)
		return false
	}
	return reply.VoteGranted
}
func (rf *Raft) RequestVote(args *RequestVoteRequest, reply *RequestVoteReply) {
	doneCh := make(chan bool)
	rf.c <- &ev{args, reply, doneCh}
	<-doneCh
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteReply, respCh chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	respCh <- reply
	return ok
}

func (rf *Raft) processAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.updateCurrentTerm(reply.Term, -1)
	}
}
func (rf *Raft) processAppendEntriesRequest(args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return false
	}

	if args.Term >= rf.currentTerm {
		rf.updateCurrentTerm(args.Term, args.LeaderId)
	}

	if rf.State() == Candidate {
		rf.SetState(Follower)
	}

	reply.Success = true
	return true
}
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	doneCh := make(chan bool)
	rf.c <- &ev{args, reply, doneCh}
	<-doneCh
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply, respCh chan *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	respCh <- reply
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
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Stopped
}

type ev struct {
	args  interface{}
	reply interface{}
	done  chan bool
}

func (rf *Raft) followerLoop() {
	electionTimeout := rf.ElectionTimeout()
	timeoutCh := afterBetween(rf.me, electionTimeout, ElectionTimeoutFactor*electionTimeout)

	for rf.State() == Follower {
		update := false
		select {
		case e := <-rf.c:
			switch req := e.args; req.(type) {
			case *RequestVoteRequest:
				update = rf.processVoteRequest(req.(*RequestVoteRequest), e.reply.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				update = rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.reply.(*AppendEntriesReply))
				e.done <- true
			}
		case <-timeoutCh:
			update = false
			rf.SetState(Candidate)
		}
		if update {
			timeoutCh = afterBetween(rf.me, electionTimeout, ElectionTimeoutFactor*electionTimeout)
		}
	}
}
func (rf *Raft) candidateLoop() {
	electionTimeout := rf.ElectionTimeout()
	var timeoutCh <-chan time.Time
	doVote := true
	voteGranted := 0
	var respCh chan *RequestVoteReply

	for rf.State() == Candidate {
		if doVote {
			rf.incrCurrentTerm()
			rf.votedFor = rf.me
			timeoutCh = afterBetween(rf.me, electionTimeout, ElectionTimeoutFactor*electionTimeout)

			respCh = make(chan *RequestVoteReply, len(rf.peers)-1)
			lastLogIndex, lastLogTerm := rf.log.lastInfo()
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(serverIdx int, ch chan *RequestVoteReply) {
						rf.sendRequestVote(serverIdx, newRequestVoteRequest(rf.CurrentTerm(), rf.me, lastLogIndex, lastLogTerm), newRequestVoteReply(), ch)
					}(i, respCh)
				}
			}

			voteGranted = 1
			doVote = false
		}

		if voteGranted == rf.QuorumSize() {
			rf.SetState(Leader)
			return
		}

		select {
		case reply := <-respCh:
			if isVoted := rf.processVoteReply(reply); isVoted {
				voteGranted++
			}
		case e := <-rf.c:
			switch req := e.args; req.(type) {
			case *RequestVoteRequest:
				rf.processVoteRequest(req.(*RequestVoteRequest), e.reply.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.reply.(*AppendEntriesReply))
				e.done <- true
			}
		case <-timeoutCh:
			doVote = true
		}
	}
}
func (rf *Raft) leaderLoop() {
	rf.setLeader(rf.me)
	rf.votedFor = -1

	heartbeatInterval := rf.HeartbeatInterval()
	var heartbeatCh <-chan time.Time
	sendHeartbeat := true
	var respCh chan *AppendEntriesReply

	for rf.State() == Leader {
		if sendHeartbeat {
			heartbeatCh = afterBetween(rf.me, heartbeatInterval, heartbeatInterval)
			respCh = make(chan *AppendEntriesReply, len(rf.peers)-1)
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(serverIdx int, ch chan *AppendEntriesReply) {
						rf.sendAppendEntries(serverIdx, newAppendEntriesRequest(rf.CurrentTerm(), rf.me, -1, -1, -1, nil), newAppendEntriesReply(), ch)
					}(i, respCh)
				}
			}
			sendHeartbeat = false
		}

		select {
		case e := <-rf.c:
			switch req := e.args; req.(type) {
			case *RequestVoteRequest:
				rf.processVoteRequest(req.(*RequestVoteRequest), e.reply.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.reply.(*AppendEntriesReply))
				e.done <- true
			}
		case resp := <-respCh:
			rf.processAppendEntriesReply(resp)
		case <-heartbeatCh:
			sendHeartbeat = true
		}
	}
}
func (rf *Raft) eventLoop() {
	state := rf.State()
	for state != Stopped {
		switch state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.State()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.leader = -1
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = &Log{}
	rf.log.entries = []*LogEntry{&LogEntry{nil, -1, 0}}

	rf.c = make(chan *ev)

	rf.SetElectionTimeout(time.Duration(300) * time.Millisecond)
	rf.SetHeartbeatInterval(time.Duration(120) * time.Millisecond)

	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.eventLoop()
	}()

	return rf
}
