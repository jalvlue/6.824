clean:
	rm -f *log

test2A-init:
	go test -timeout 10s -race -run ^TestInitialElection2A$$ 6.5840/raft

test2A-re:
	go test -timeout 10s -race -run ^TestReElection2A$$ 6.5840/raft

test2A-many:
	go test -timeout 30s -race -run ^TestManyElections2A$$ 6.5840/raft

test2A:
	go test -run 2A

test2B-basicAgree:
	go test -timeout 10s -race -run ^TestBasicAgree2B$$ 6.5840/raft

test2B-RPCBytes:
	go test -timeout 10s -race -run ^TestRPCBytes2B$$ 6.5840/raft

test2B-followerFailure:
	go test -timeout 10s -race -run ^TestFollowerFailure2B$$ 6.5840/raft

test2B-leaderFailure:
	go test -timeout 10s -race -run ^TestLeaderFailure2B$$ 6.5840/raft

test2B-failAgree:
	go test -timeout 10s -race -run ^TestFailAgree2B$$ 6.5840/raft

test2B-failNoAgree:
	go test -timeout 10s -race -run ^TestFailNoAgree2B$$ 6.5840/raft

test2B-concurrentStarts:
	go test -timeout 10s -race -run ^TestConcurrentStarts2B$$ 6.5840/raft

test2B-reJoin:
	go test -timeout 10s -race -run ^TestReJoin2B$$ 6.5840/raft

test2B-backup:
	go test -timeout 10s -race -run ^TestBackup2B$$ 6.5840/raft

test2B-count:
	go test -timeout 10s -race -run ^TestCount2B$$ 6.5840/raft

test2B:
	go test -run 2B

.PHONY: clean test2A-init test2A-re test2A-many test2A test2B-basicAgree test2B-RPCBytes test2B-followerFailure test2B-leaderFailure test2B-failAgree test2B-failNoAgree test2B-concurrentStarts test2B-reJoin test2B-backup test2B-count test2B
