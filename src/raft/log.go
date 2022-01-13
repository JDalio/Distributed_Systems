package raft

import (
	huge "github.com/dablelv/go-huge-util"
	"sync"
)

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

func (l *Log) appendOne(command interface{}, term int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	index := len(l.entries)
	entry := newLogEntry(command, index, term)
	l.entries = append(l.entries, entry)

	str, _ := huge.ToIndentJSON(entry)
	DPrintf("\n---Append One---\n%v\n", str)

	return index
}

func (l *Log) appendMany(entries []*LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, entry := range entries {
		idx := entry.Index
		term := entry.Term
		if idx < len(l.entries) && l.entries[idx].Term == term {
			l.entries[idx] = entry
		} else {
			l.entries = append(l.entries, entry)
		}
	}
}
func (l *Log) show() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	str, _ := huge.ToIndentJSON(l.entries)
	return str
}
func (l *Log) hasLog(prevLogIndex int, prevLogTerm int) bool {
	l.mu.RLock()
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

func (l *Log) getBetween(from int, to int) []*LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[from:to]
}
