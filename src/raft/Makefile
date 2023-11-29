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

.PHONY: clean test2A-init test2A-re test2A-many test2A
