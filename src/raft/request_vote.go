package raft

type RequestVoteRequest struct {
	// Your data here (2A, 2B).
	Term int

	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func newRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{}
}

//thread unsafe
func newRequestVoteRequest(term int, candidateId int, lastLogIndex int, lastLogTerm int) *RequestVoteRequest {
	return &RequestVoteRequest{
		term,
		candidateId,
		lastLogIndex,
		lastLogTerm,
	}
}
