.DEFAULT_GOAL := all

OS := $(shell uname)

DOCKERFILE_MD5SUM=$(shell md5sum ./Dockerfile | cut -d" " -f1)
DEV_DOCKER_IMAGE := csilvm_dev:$(DOCKERFILE_MD5SUM)

ifeq ($(OS), Linux)
DOCKER ?= yes
else ifeq ($(OS), Darwin)
  ifeq ($(MAKECMDGOALS), check)
  DOCKER ?= yes
  else
  DOCKER ?= no
  endif
else
$(error Unsupported OS '$(OS)')
endif

.DEFAULT_GOAL := all

.PHONY: dev_image build check all clean

dev_image:
	docker inspect $(DEV_DOCKER_IMAGE) &> /dev/null || docker build --rm -t $(DEV_DOCKER_IMAGE) .

ifeq ($(DOCKER), yes)
TEST_PREFIX := docker run --rm $(DEV_DOCKER_IMAGE)
BUILD_PREFIX := docker run --rm -v `pwd`:/go/src/github.com/mesosphere/csilvm $(DEV_DOCKER_IMAGE)

build: dev_image
check: dev_image
gofmt: dev_image

shell: dev_image
	docker run --rm -ti -v `pwd`:/go/src/github.com/mesosphere/csilvm $(DEV_DOCKER_IMAGE) /bin/bash
endif

check:
	$(BUILD_PREFIX) sh -c "go build -v ./... && gometalinter --config=gometalinter.conf --vendor ./..."

build:
	$(BUILD_PREFIX) go build ./cmd/csilvm

gofmt:
	$(BUILD_PREFIX) sh -c "find pkg -name '*.go' | xargs gofmt -s -w"
	$(BUILD_PREFIX) sh -c "find cmd -name '*.go' | xargs gofmt -s -w"

all: build

.PHONY: sudo-test
sudo-test:
	go test -c -i ./pkg/lvm
	sudo ./lvm.test -test.v
	go test -c -i ./pkg/csilvm
	sudo ./csilvm.test -test.v
