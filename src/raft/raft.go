package raft

import (
	huge "github.com/dablelv/go-huge-util"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// GetState Entry,return currentTerm and whether this server believes it is the leader.
// {@race rf.leader, since rf.me will not change}
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.leader == rf.me
}

// RequestVote Routine Entry, communicate with main loop using channel, no race
func (rf *Raft) RequestVote(args *RequestVoteRequest, reply *RequestVoteReply) {
	doneCh := make(chan bool)
	rf.c <- &ev{args, reply, doneCh}
	<-doneCh
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteReply, respCh chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	respCh <- reply
	return ok
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
	isUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
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

// AppendEntries Rpc Routine Entry, communicate with main loop using channel, no race
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	doneCh := make(chan bool)
	rf.c <- &ev{args, reply, doneCh}
	<-doneCh
}

// Send AppendEntries/Heartbeat Routine Entry, Only for Leader
func (rf *Raft) sendAppendEntries(respCh chan *ev) {
	for i, _ := range rf.peers {
		if i != rf.me {
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(i)
			// Ensure entries starts from prevLogIndex
			entries := rf.log.getBetween(prevLogIndex+1, -1)
			go func(serverIdx int, request *AppendEntriesRequest, ch chan *ev) {
				reply := newAppendEntriesReply()
				ok := rf.peers[serverIdx].Call("Raft.AppendEntries", request, reply)
				if !ok {
					return
				}
				res := &ev{reply, request, nil}

				rf.log.mu.RLock()
				if request.Entries != nil {
					replyStr, _ := huge.ToIndentJSON(reply)
					reqStr, _ := huge.ToIndentJSON(request)
					DPrintf("---------->AppendEntries %d---\n%v\n%v\n", serverIdx, reqStr, replyStr)
				}
				rf.log.mu.RUnlock()

				respCh <- res
			}(i,
				newAppendEntriesRequest(rf.CurrentTerm(), rf.me, rf.CommitIndex(), prevLogIndex, prevLogTerm, entries),
				respCh)
		}
	}
}

// @return Whether Follower/Candidate should reset election timeout
func (rf *Raft) processAppendEntriesRequest(args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	reply.Term = rf.CurrentTerm()
	reply.Me = rf.me

	//Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm() {
		reply.Success = false
		//If AppendEntries RPC received from new leader:
		//convert to follower
		if rf.State() == Candidate {
			rf.updateCurrentTerm(rf.CurrentTerm(), args.LeaderId)
			return true
		}
		return false
	}

	rf.updateCurrentTerm(args.Term, args.LeaderId)

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !rf.log.hasLog(args.PrevLogIndex, args.PrevLogTerm) {
		//logLength := rf.log.length()
		//--- Fast Backup Start ---//
		//if args.PrevLogIndex > logLength {
		//	reply.XTerm = -1
		//	reply.XIndex = -1
		//	reply.XLen = args.PrevLogIndex - logLength
		//} else {
		//	reply.XTerm = rf.log.getLogEntryTerm(args.PrevLogIndex)
		//	reply.XIndex = rf.log.termFirstIndex(reply.XTerm)
		//	reply.XLen = -1
		//}
		//--- Fast Backup End ---//
		//rf.log.deleteFrom(args.PrevLogIndex)
		reply.Success = false
		return true
	}

	reply.Success = true

	rf.log.overwrite(args.Entries)

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	cidx := rf.CommitIndex()
	if args.LeaderCommit > cidx {
		lastIndex, _ := rf.log.lastInfo()
		DPrintf("---------->argsIndex:%d commitIndex:%d lastIndex:%d", args.LeaderCommit, cidx, lastIndex)

		if lastIndex < args.LeaderCommit {
			rf.setCommitIndex(lastIndex)
		} else {
			rf.setCommitIndex(args.LeaderCommit)
		}
	}

	rf.apply()
	return true
}
func (rf *Raft) processAppendEntriesReply(reply *AppendEntriesReply, args *AppendEntriesRequest) {
	if reply.Term > rf.CurrentTerm() {
		rf.updateCurrentTerm(reply.Term, -1)
		return
	}

	if reply.Success {
		rf.updateFollowerIndex(reply.Me, args.PrevLogIndex, len(args.Entries))
	} else {
		rf.decrNextIndex(reply.Me)
		//rf.fastBackup(reply.Me, args.PrevLogIndex, reply.XIndex, reply.XTerm, reply.XLen)
	}

	lastIndex, _ := rf.log.lastInfo()
	cmtIndex := rf.CommitIndex()
	for cmtIndex++; lastIndex > 0 && cmtIndex <= lastIndex; {
		quorumNum := 1
		for i, _ := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= cmtIndex {
				quorumNum++
			}
		}
		if quorumNum >= rf.QuorumSize() {
			cmtIndex++
		} else {
			cmtIndex--
			break
		}
		DPrintf("[Quorum] Index:%d Size:%d", rf.CommitIndex()-1, quorumNum)
	}

	if cmtIndex > lastIndex {
		cmtIndex--
	}
	rf.setCommitIndex(cmtIndex)

	rf.apply()
}

// If commitIndex > lastApplied: increment lastApplied,
// apply log[lastApplied] to state machine
func (rf *Raft) apply() {
	commitIndex := rf.CommitIndex()
	if commitIndex > rf.lastApplied {
		for rf.lastApplied++; rf.lastApplied <= commitIndex; rf.lastApplied++ {
			entry := rf.log.get(rf.lastApplied)

			str, _ := huge.ToIndentJSON(entry)
			_, isLearder := rf.GetState()
			DPrintf("---------->Apply Me:%d isLeader:%t CommitIndex:%d LastApplied:%d---\n%v\n", rf.me, isLearder, commitIndex, rf.lastApplied, str)

			rf.applyCh <- newApplyMsg(true, *entry, false, nil, 0, -1)
		}
		rf.lastApplied--
	}
}

func (rf *Raft) peerNum() int {
	return len(rf.peers)
}
func (rf *Raft) Show() {
	term, isLeader := rf.GetState()
	DPrintf("---------->\nMe:%d Term:%d Leader:%t CommitIndex:%d\n---Logs---\n%v\n----------\n", rf.me, term, isLeader, rf.CommitIndex(), rf.log.show())
}

// StartCommand Routine Entry
// {@race rf.log, rf.nextIndex, rf.commitIndex}
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	term, isLeader = rf.GetState()

	DPrintf("---Me:%d isLeader: %t---", rf.me, isLeader)

	if !isLeader {
		return
	}

	// Your code here (2B).
	index = rf.log.appendOne(command, term)
	rf.sendAppendEntries(rf.c)
	return
}

// Kill Loop Routine Entry
// {@race rf.state}
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Stopped
}

type ev struct {
	args  interface{}
	value interface{}
	done  chan bool
}

// Main Loop Entry
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
func (rf *Raft) followerLoop() {
	DPrintf("[Become Follower] %d", rf.me)
	timeoutCh := afterBetween(rf.me, ElectionTimeout, ElectionTimeoutFactor*ElectionTimeout)

	for rf.State() == Follower {
		update := false
		select {
		case e := <-rf.c:
			switch req := e.args; req.(type) {
			case *RequestVoteRequest:
				update = rf.processVoteRequest(req.(*RequestVoteRequest), e.value.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				update = rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.value.(*AppendEntriesReply))
				e.done <- true
			}
		case <-timeoutCh:
			update = false
			rf.SetState(Candidate)
		}
		if update {
			timeoutCh = afterBetween(rf.me, ElectionTimeout, ElectionTimeoutFactor*ElectionTimeout)
		}
	}
}
func (rf *Raft) candidateLoop() {
	DPrintf("[Become Candidate] %d", rf.me)
	var timeoutCh <-chan time.Time
	doVote := true
	voteGranted := 0
	var respCh chan *RequestVoteReply

	for rf.State() == Candidate {
		if doVote {
			rf.incrCurrentTerm()
			rf.votedFor = rf.me
			timeoutCh = afterBetween(rf.me, ElectionTimeout, ElectionTimeoutFactor*ElectionTimeout)

			respCh = make(chan *RequestVoteReply, len(rf.peers)-1)
			lastLogIndex, lastLogTerm := rf.log.lastInfo()
			for i, _ := range rf.peers {
				if i != rf.me {
					// race with main loop {@race rf.currentTerm}
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
				rf.processVoteRequest(req.(*RequestVoteRequest), e.value.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.value.(*AppendEntriesReply))
				e.done <- true
			}
		case <-timeoutCh:
			doVote = true
		}
	}
}
func (rf *Raft) leaderLoop() {
	DPrintf("[Become Leader] %d", rf.me)
	rf.setLeader(rf.me)
	rf.votedFor = -1
	rf.initLogIndex()

	sendHeartbeat := true
	var heartbeatCh <-chan time.Time
	var heartbeatRespCh chan *ev

	for rf.State() == Leader {
		if sendHeartbeat {
			heartbeatCh = afterBetween(rf.me, HeartbeatInterval, HeartbeatInterval)
			heartbeatRespCh = make(chan *ev, len(rf.peers)-1) // with cache so abandoned channel will not panic
			rf.sendAppendEntries(heartbeatRespCh)
			sendHeartbeat = false
		}

		select {
		case e := <-rf.c:
			switch req := e.args; req.(type) {
			case *RequestVoteRequest:
				rf.processVoteRequest(req.(*RequestVoteRequest), e.value.(*RequestVoteReply))
				e.done <- true
			case *AppendEntriesRequest:
				rf.processAppendEntriesRequest(req.(*AppendEntriesRequest), e.value.(*AppendEntriesReply))
				e.done <- true
			case *AppendEntriesReply:
				rf.processAppendEntriesReply(req.(*AppendEntriesReply), e.value.(*AppendEntriesRequest))
			}
		case heartbeatResp := <-heartbeatRespCh:
			rf.processAppendEntriesReply(heartbeatResp.args.(*AppendEntriesReply), heartbeatResp.value.(*AppendEntriesRequest))
		case <-heartbeatCh:
			sendHeartbeat = true
		}
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
	rf.log.entries = []*LogEntry{newLogEntry(nil, 0, -1)}

	rf.applyCh = applyCh
	rf.appendEntriesRespChan = make(chan *AppendEntriesReply)
	rf.c = make(chan *ev)

	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.eventLoop()
	}()

	return rf
}
