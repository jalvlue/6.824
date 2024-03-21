package shardctrler

import (
	"log"
	"math"

	"6.5840/kvraft"
)

var Debug bool

func init() {
	Debug = kvraft.Debug

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// collect keys of a map to a slice
func mapToSlice(m map[int][]string) []int {
	slice := []int{}
	for key := range m {
		slice = append(slice, key)
	}

	return slice
}

// compare func
func isGreater(a, b int) int {
	if a > b {
		return 1
	} else if a == b {
		return 0
	}
	return -1
}
func isLess(a, b int) int {
	if a < b {
		return 1
	} else if a == b {
		return 0
	}
	return -1
}

// quick sort partition
func partition(groups []int, groupLoads map[int]int, low, high int, cmp func(int, int) int) int {
	pivotGID := groups[high]
	pivot := groupLoads[pivotGID]
	i := low - 1

	for j := low; j < high; j++ {
		// divide GID with higher/lower loads into two parts
		// for GID with loads as pivot, divide by GIDs for consistency
		if res := cmp(groupLoads[groups[j]], pivot); res == 1 || (res == 0 && pivotGID > groups[j]) {
			i += 1
			groups[i], groups[j] = groups[j], groups[i]
		}
	}

	groups[i+1], groups[high] = groups[high], groups[i+1]
	return i + 1
}

// quick sort
func sortGroupByLoads(groups []int, groupLoads map[int]int, low, high int, cmp func(int, int) int) {
	if high > low {
		mid := partition(groups, groupLoads, low, high, cmp)
		sortGroupByLoads(groups, groupLoads, low, mid-1, cmp)
		sortGroupByLoads(groups, groupLoads, mid+1, high, cmp)
	}
}

// if a group's load is bigger than averageLoad+1, then it should move loads to other groups
func needToMove(originLoad int, averageLoad float64) bool {
	ceiled := int(math.Ceil(averageLoad))
	couldMove := originLoad > ceiled
	// log.Printf("originLoad: %v, averageLoad: %v, ceiled: %v, couldMove: %v\n", originLoad, averageLoad, ceiled, couldMove)

	return couldMove
}

// move a shard load of originGroup to newGroup
func moveOneShardTo(shards *[NShards]int, originGID, newGID int) {
	for i, GID := range shards {
		if GID == originGID {
			shards[i] = newGID
			return
		}
	}
}
