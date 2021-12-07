package raft

import "sync"

type Log struct {
	mu sync.RWMutex
	// push LogEntry{ Term:0, Index:0} when initializing
	entries []*LogEntry
}

func (l *Log) lastInfo() (index int, term int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	entry := l.entries[len(l.entries)-1]
	term = entry.Term
	index = entry.Index
	return
}
