.PHONY: all al7-build

ALL = al7-build
TARGETS = $(foreach dir,$(ALL),$(dir)/.docker)

all: $(filter-out al7-deps/.docker, $(TARGETS))

push:
	docker tag $(BUILD_AGENT) $(REGISTRY)/$(BUILD_AGENT_NS)
	docker push $(REGISTRY)/$(BUILD_AGENT_NS)

include ../common.mk
-include ../local.mk
