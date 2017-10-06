.DEFAULT_GOAL := all

.PHONY: test
test:
	go test -v .

.PHONY: sudo-test
sudo-test:
	go test -c -i .
	sudo ./csilvm.test -test.v
	go test -c -i ./lvm
	sudo ./lvm.test -test.v
	go test -c -i ./lvs
	sudo ./lvs.test -test.v

.PHONY: all
all: sudo-test
	go build -v .
