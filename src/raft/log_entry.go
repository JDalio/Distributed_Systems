package raft

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

func newLogEntry(command interface{}, index int, term int) *LogEntry {
	return &LogEntry{command, index, term}
}
