SHELL = /bin/bash

.PHONY: all ub18-build ub18-kvm

IMAGE = $(UB18)
KVM_GROUP := $(shell stat -c %g /dev/kvm 2>/dev/null || echo 129)

include ../common.mk
-include ../local.mk

XCALAR_THRIFT_VERSION = 0.10.0-75
XCALAR_PROTOBUF_VERSION = 3.6.1-75

default: ub18-build/.docker

all: ub18-build/.docker
