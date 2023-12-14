clean:
	rm -f *log

test2A:
	go test -run 2A

test2B:
	go test -run 2B

dstest2A:
	./dstest -r -v -p 20 -n 100 TestInitialElection2A TestReElection2A TestManyElections2A

dstest2B:
	./dstest -r -v -p 10 -n 5 TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestReJoin2B TestBackup2B TestCount2B

dstest2B-pressure:
	./dstest -r -v -p 20 -n 100 TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestReJoin2B TestBackup2B TestCount2B

.PHONY: clean test2A test2B dstest2A dstest2B dstest2B-pressure
