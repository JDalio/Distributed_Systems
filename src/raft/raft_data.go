package raft

import (
	"6.824/labrpc"
	"fmt"
	"sync"
	"time"
)

const (
	ElectionTimeoutFactor = 4
	ElectionTimeout       = time.Duration(300) * time.Millisecond
	HeartbeatInterval     = time.Duration(120) * time.Millisecond
)

const (
	Stopped   = -1
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// race means other routine race with main loop
	me          int // this peer's index into peers[]
	state       int //race
	leader      int //race
	currentTerm int //race
	votedFor    int

	log                   *Log //race
	appendEntriesRespChan chan *AppendEntriesReply

	// Volatile state on all servers
	lastApplied int // 0... race at rf.apply
	commitIndex int // 0...

	// Volatile state on leaders. Reinitialized after election
	nextIndex  []int
	matchIndex []int //use in copy stage

	applyCh chan ApplyMsg
	c       chan *ev
}

func (rf *Raft) getPrevLogInfo(serverIdx int) (prevLogIndex int, prevLogTerm int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	prevLogIndex = rf.nextIndex[serverIdx] - 1
	prevLogTerm = rf.log.getLogEntryTerm(prevLogIndex)
	return
}
func (rf *Raft) getAppendEntries(serverIdx int) []*LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	lastIndex, _ := rf.log.lastInfo()
	nextIndex := rf.nextIndex[serverIdx]
	if nextIndex > lastIndex {
		return nil
	}
	return rf.log.entries[nextIndex:lastIndex]
}
func (rf *Raft) LastApplied() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastApplied
}
func (rf *Raft) CommitIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.commitIndex
}
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
}

// sync apply command to state machine
func (rf *Raft) apply(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entry := rf.log.get(index)
	fmt.Printf("[Apply LogEntry] %s", entry.Command)
	rf.lastApplied++
}
func (rf *Raft) NextIndex(serverIdx int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.nextIndex[serverIdx]
}
func (rf *Raft) initLogIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	index, _ := rf.log.lastInfo()

	for i, _ := range rf.peers {
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = 0
	}
}

// leader race option
func (rf *Raft) setLeader(leader int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = leader
}

// state race option
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

// no race
func (rf *Raft) QuorumSize() int {
	return len(rf.peers)/2 + 1
}

// currentTerm race option
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
	if state != Follower {
		rf.SetState(Follower)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.leader = leaderId
	rf.votedFor = -1
}