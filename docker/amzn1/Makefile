.PHONY: all default minimal amzn1-build amzn1-offline

IMAGE = $(AMZN1)

MINIMAL = amzn1-offline
ALL = $(MINIMAL) amzn1-build

TARGETS = $(foreach dir,$(ALL),$(dir)/.docker)

default: minimal

minimal: $(foreach dir,$(MINIMAL),$(dir)/.docker)

all: $(filter-out amzn1-deps/.docker, $(TARGETS))

include ../common.mk
-include ../local.mk
