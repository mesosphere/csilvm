.DEFAULT_GOAL := all

.PHONY: test
test:
	go test -v .
	go test -v ./lvs

.PHONY: sudo-test
sudo-test:
	go test -c -i .
	sudo ./csilvm.test -test.v
	go test -c -i ./lvm
	sudo ./lvm.test -test.v
	go test ./lvs -test.v

.PHONY: all
all: sudo-test
	go build -v .
