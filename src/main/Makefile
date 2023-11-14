wc:
	go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

mrcoordinator:
	go build -race -gcflags="all=-N -l" mrcoordinator.go
	./mrcoordinator pg-*.txt

mrworker:
	go build -race -gcflags="all=-N -l" mrworker.go
	./mrworker wc.so

clean_wc:
	rm -f wc.so mr-inter-* mr-out-* mrcoordinator mrworker

.PHONY: wc mrcoordinator mrworker clean_wc
