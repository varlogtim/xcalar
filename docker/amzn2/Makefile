.PHONY: all default minimal amzn2-build amzn2-offline

SHELL = /bin/bash

MINIMAL = amzn2-offline
ALL = $(MINIMAL) amzn2-build

IMAGE = $(AMZN2)

TARGETS = $(foreach dir,$(ALL),$(dir)/.docker)

default: minimal

minimal: $(foreach dir,$(MINIMAL),$(dir)/.docker)

all: $(filter-out amzn2-deps/.docker, $(TARGETS))

XCALAR_THRIFT_VERSION=0.10.0-75.amzn2
XCALAR_CLANG_VERSION=5
#OPTCLANG=/opt/clang5


include ../common.mk
-include ../local.mk

