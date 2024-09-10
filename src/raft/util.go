package raft

import (
	"log"
	"os"
)

// Debugging
var Debug = os.Getenv("DIST_SYS_DEBUG") == "1"

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
