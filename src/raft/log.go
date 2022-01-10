package raft

import "sync"

type Log struct {
	mu sync.RWMutex
	// push LogEntry{ Term:-1, Index:0} when initializing
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

func (l *Log) getLogEntryTerm(logIndex int) (term int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[logIndex].Term
}

func (l *Log) appendOne(command interface{}, index int, term int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, newLogEntry(command, index, term))
}

func (l *Log) appendMany(entries []*LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

func (l *Log) hasLog(prevLogIndex int, prevLogTerm int) bool {
	l.mu.RUnlock()
	defer l.mu.RUnlock()
	if prevLogIndex > len(l.entries)-1 || prevLogTerm != l.entries[prevLogIndex].Term {
		return false
	}
	return true
}

func (l *Log) deleteFrom(index int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = l.entries[0:index]
}

func (l *Log) get(index int) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[index]
}
