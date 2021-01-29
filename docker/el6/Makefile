.PHONY: all el6-build el6-offline el6-deps

ALL = el6-base el6-build el6-offline el6-deps
IMAGE = $(EL6_BASE)

TARGETS = $(foreach dir,$(ALL),$(dir)/.docker)

all: $(filter-out el6-deps/.docker, $(TARGETS))

el6-base/.docker: el6-base/Dockerfile
	docker build --build-arg=IMAGE=$(CENTOS6) -t $(@D) -f $< $(@D)

el6-deps/.docker: el6-deps/Dockerfile $(XLRDIR)/pkg/node/package.json
	docker build -t $(@D) -f $< \
           --build-arg=XCALAR_PYTHON_VERSION=$(XCALAR_PYTHON_VERSION) \
           --build-arg=IMAGE=$(IMAGE) \
           --build-arg=http_proxy=$(http_proxy) $(XLRDIR)

include ../common.mk
-include ../local.mk
