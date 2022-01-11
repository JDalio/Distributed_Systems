package raft

type AppendEntriesRequest struct {
	Term int

	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []*LogEntry
}

type AppendEntriesReply struct {
	Me      int
	Term    int
	Success bool
}

func newAppendEntriesReply() *AppendEntriesReply {
	return &AppendEntriesReply{}
}

func newAppendEntriesRequest(term int, leaderId int, leaderCommit int, prevLogIndex int, prevLogTerm int, entries []*LogEntry) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		term,
		leaderId,
		leaderCommit,
		prevLogIndex,
		prevLogTerm,
		entries,
	}
}
