package raft

import (
	"6.824/labgob"
	"bytes"
)

type Persist struct {
	LogEntries  []*LogEntry
	VotedFor    int
	CurrentTerm int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	p := Persist{rf.log.getBetween(0, -1), rf.votedFor, rf.currentTerm}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(p)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var p Persist

	if d.Decode(&p) == nil {
		rf.log.entries = p.LogEntries
		rf.votedFor = p.VotedFor
		rf.currentTerm = p.CurrentTerm
	}
}
