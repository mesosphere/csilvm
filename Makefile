.DEFAULT_GOAL := all

OS := $(shell uname)

DEV_DOCKER_IMAGE := csilvm_dev

ifeq ($(OS), Linux)
DOCKER ?= no
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
	docker build --rm -t $(DEV_DOCKER_IMAGE) .

ifeq ($(DOCKER), yes)
TEST_PREFIX := docker run --rm $(DEV_DOCKER_IMAGE)
BUILD_PREFIX := docker run --rm $(DEV_DOCKER_IMAGE)

build: dev_image
check: dev_image
endif

build:
	$(BUILD_PREFIX) go build -o lvs-bin -v ./cmd/lvs/

all: build

.PHONY: sudo-test
sudo-test:
	go test -c -i ./pkg/lvm
	sudo ./lvm.test -test.v
	go test -c -i ./pkg/csilvm
	sudo ./csilvm.test -test.v
