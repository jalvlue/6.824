test2A:
	go test -run 2A

test2B:
	go test -run 2B

test2C:
	go test -run 2C

test2D:
	go test -run 2D

dstest2A:
	./dstest -r -v -p 20 -n 100 TestInitialElection2A TestReElection2A TestManyElections2A

dstest2B:
	./dstest -r -v -p 20 -n 100 TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestReJoin2B TestBackup2B TestCount2B

dstest2C:
	./dstest -r -v -p 20 -n 100 TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C

dstest2D:
	./dstest -r -v -p 20 -n 100 TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D TestSnapshotInstallUnCrash2D TestSnapshotAllCrash2D TestSnapshotInit2D

.PHONY: test2A test2B test2C dstest2A dstest2B dstest2C dstest2D
