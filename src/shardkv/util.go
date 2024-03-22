package shardkv

import (
	"log"
	"os"
	"strconv"
)

var Debug bool

func getDebugState() bool {
	level := 0
	if v, _ := strconv.Atoi(os.Getenv("VERBOSE")); v > 0 {
		level = 1
	}

	return level != 0
}

func init() {
	Debug = getDebugState()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}
