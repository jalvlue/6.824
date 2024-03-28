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

// check if I own this shard
func isMyShard(myShards map[int]struct{}, shardID int) bool {
	_, ok := myShards[shardID]
	return ok
}

// get in and out shards between old config and new config
func getInAndLeaveShards(oldMyShards, newMyShards map[int]struct{}) (map[int]struct{}, map[int]struct{}) {
	inShards := map[int]struct{}{}
	leaveShards := map[int]struct{}{}

	for oldShardID := range oldMyShards {
		if _, ok := newMyShards[oldShardID]; !ok {
			leaveShards[oldShardID] = struct{}{}
		}
	}

	for newShardID := range newMyShards {
		if !isMyShard(oldMyShards, newShardID) {
			inShards[newShardID] = struct{}{}
		}
	}

	return inShards, leaveShards
}
