package raft

import (
	"log"
	"os"
)

// Debugging
var Debug = os.Getenv("DIST_SYS_DEBUG") == "1"

func init() {
    // Add microseconds to the default log format
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
