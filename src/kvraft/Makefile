test3A:
	go test -run 3A

test3B:
	go test -run 3B

dstest3A:
	../raft/dstest -r -v -p 10 -n 100 TestBasic3A TestSpeed3A TestConcurrent3A TestUnreliable3A TestUnreliableOneKey3A TestOnePartition3A TestManyPartitionsOneClient3A TestManyPartitionsManyClients3A TestPersistOneClient3A TestPersistConcurrent3A TestPersistConcurrentUnreliable3A TestPersistPartition3A TestPersistPartitionUnreliable3A TestPersistPartitionUnreliableLinearizable3A

dstest3B:
	../raft/dstest -r -v -p 10 -n 100 TestSnapshotRPC3B TestSnapshotSize3B TestSpeed3B TestSnapshotRecover3B TestSnapshotRecoverManyClients3B TestSnapshotUnreliable3B TestSnapshotUnreliableRecover3B TestSnapshotUnreliableRecoverConcurrentPartition3B TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B

.PHONY: test3A test3B dstest3A dstest3B
