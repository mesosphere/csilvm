.DEFAULT_GOAL := all

OS := $(shell uname)

DOCKERFILE_MD5SUM=$(shell md5sum ./Dockerfile | cut -d" " -f1)
DEV_DOCKER_IMAGE := csilvm_dev:$(DOCKERFILE_MD5SUM)

PLUGIN_NAME ?= io.mesosphere.csi.lvm
PLUGIN_VERSION ?= v0.3-dev
BUILD_TIME ?= $(shell date +"%Y%m%dT%H%M%S.%N%z")
PACKAGE_SHA ?= nosha

PKG_ROOT := github.com/mesosphere/csilvm

LDFLAGS ?= \
       -X $(PKG_ROOT)/pkg/version/internal/versiondata.Product=$(PLUGIN_NAME) \
       -X $(PKG_ROOT)/pkg/version/internal/versiondata.Version=$(PLUGIN_VERSION) \
       -X $(PKG_ROOT)/pkg/version/internal/versiondata.BuildTime=$(BUILD_TIME) \
       -X $(PKG_ROOT)/pkg/version/internal/versiondata.BuildSHA=$(PACKAGE_SHA)

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

.PHONY: dev-image build check all clean shell rebuild-dev-image gofmt

rebuild-dev-image:
	docker build --rm -t $(DEV_DOCKER_IMAGE) .

dev-image:
	docker inspect $(DEV_DOCKER_IMAGE) > /dev/null || docker build --rm -t $(DEV_DOCKER_IMAGE) .

ifeq ($(DOCKER), yes)
TEST_PREFIX := \
	docker run -t --rm --privileged --tmpfs /run --tmpfs /tmp \
	-v /var/lock/lvm:/var/lock/lvm -v `pwd`:/go/src/$(PKG_ROOT) --ipc=host \
	--pid=host --net=host $(DEV_DOCKER_IMAGE)

BUILD_PREFIX := \
	docker run -t --rm -v `pwd`:/go/src/$(PKG_ROOT) $(DEV_DOCKER_IMAGE)

build: dev-image
check: dev-image
gofmt: dev-image

shell: dev-image
	docker run --rm -ti -v `pwd`:/go/src/$(PKG_ROOT) $(DEV_DOCKER_IMAGE) /bin/bash
endif

check:
	$(BUILD_PREFIX) sh -c "go build -v ./... && gometalinter --config=gometalinter.conf --vendor ./..."

build:
	$(BUILD_PREFIX) go build -ldflags "$(LDFLAGS)" ./cmd/csilvm

gofmt:
	$(BUILD_PREFIX) sh -c "find pkg -name '*.go' | xargs gofmt -s -w"
	$(BUILD_PREFIX) sh -c "find cmd -name '*.go' | xargs gofmt -s -w"

all: build

.PHONY: sudo-test
sudo-test: MKNOD=$(shell for i in 0 1 2 3 4 5 6 7 8; do echo "(test -e /dev/loop$$i || mknod -m 0660 /dev/loop$$i b 7 $$i) &&"; done)
sudo-test: dev-image
	$(TEST_PREFIX) sh -c "$(MKNOD) go test -race -c -i ./pkg/lvm && ./lvm.test -test.v -test.run=${FILTER}"
	$(TEST_PREFIX) sh -c "$(MKNOD) go test -race -c -i ./pkg/csilvm && ./csilvm.test -test.v -test.run=${FILTER}"
