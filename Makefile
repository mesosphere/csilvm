.DEFAULT_GOAL := all

.PHONY: test
test:
	go test -v .

.PHONY: sudo-test
sudo-test:
	go test -c -i .
	go test -c -i ./lvm
	sudo ./csilvm.test -test.v
	sudo ./lvm.test -test.v

.PHONY: all
all: sudo-test
	go build -v .
