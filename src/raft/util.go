package raft

import (
	"log"
	"math/rand"
	"strings"
	"time"
)

type mytype time.Time

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	} else if strings.HasPrefix(format, "no---------->") {
		log.Printf(format, a...)
	}
	return
}

func afterBetween(id int, min time.Duration, max time.Duration) <-chan time.Time {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() * int64(id)))
	d, delta := min, max-min
	if delta > 0 {
		d += time.Duration(rnd.Int63n(int64(delta)))
	}
	return time.After(d)
}
