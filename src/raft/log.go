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

func (l *Log) termFirstIndex(term int) int {
	for _, l := range l.entries {
		if l.Term == term {
			return l.Index
		}
	}
	return -1
}

// 0位置的不算, 有效log长度
func (l *Log) length() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries) - 1
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

func (l *Log) overwrite(entries []*LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If an existing entry conflicts with a new one(same index but diff terms),
	// delete the existing entry and all that follow it
	for _, e := range entries {
		if e.Index < len(l.entries) && l.entries[e.Index].Term != e.Term {
			l.entries = l.entries[:e.Index]
		}
	}

	// Append any new entries not already in the log
	for i, e := range entries {
		if e.Index >= len(l.entries) {
			l.entries = append(l.entries, entries[i:]...)
			break
		}
	}
}

func (l *Log) show() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	str, _ := huge.ToIndentJSON(l.entries)
	return str
}
func (l *Log) hasLog(index int, term int) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index > len(l.entries)-1 || term != l.entries[index].Term {
		return false
	}
	return true
}

func (l *Log) get(index int) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[index]
}

func (l *Log) getBetween(from int, to int) []*LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copyEntries := make([]*LogEntry, len(l.entries))
	copy(copyEntries, l.entries)

	if to == -1 {
		return copyEntries[from:]
	}
	return copyEntries[from:to]
}
