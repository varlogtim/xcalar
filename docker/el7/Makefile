SHELL = /bin/bash

.PHONY: all default minimal el7-base el7-systemd el7-build el7-offline el7-build-agent el7-deps el7-vsts-agent

MINIMAL = el7-base el7-systemd el7-build el7-offline
ALL = $(MINIMAL) el7-build-agent

TARGETS = $(foreach dir,$(ALL),$(dir)/.docker)

include ../common.mk

default: minimal

minimal: $(foreach dir,$(MINIMAL),$(dir)/.docker)

all: $(TARGETS)

el7-build-agent/.docker: el7-build/.docker
	@echo Building $(@D) ...
	@cd $(@D) && make build

el7-deps/.docker: el7-deps/Dockerfile $(XLRDIR)/pkg/node/package.json
	$(E)tar cf - -C $(XLRDIR) docker/el7/el7-deps/Dockerfile $$(grep -E '(ADD|COPY)' el7-deps/Dockerfile | awk '{print $$2}' | sort | uniq) | \
    $(E)$(DOCKER) build  $(Q) -t $(@D) -f docker/el7/$< \
    	--build-arg=IMAGE=$(IMAGE)  \
       	--build-arg=XCALAR_PYTHON_VERSION=$(XCALAR_PYTHON_VERSION) \
       	--build-arg=http_proxy=$(http_proxy) -

el7-deps.tar: el7-deps/.docker
	-$(DOCKER) volume rm el7-src
	$(DOCKER) volume create --name el7-src
	$(DOCKER) volume ls | grep -q el7-ccache || $(DOCKER) volume create --name=el7-ccache
	$(DOCKER) run --rm -v el7-src:/usr/src -w /usr/src -e http_proxy -v el7-ccache:/ccache -e CCACHE_DIR=/ccache $(^D) /usr/bin/build-all.sh
	$(DOCKER) run --rm -v el7-src:/usr/src -w /usr/src --entrypoint=/bin/bash $(^D) -c 'find . -mindepth 1 -maxdepth 1 -type f \( -name "*.deb" -o -name "*.rpm" \) | tar c --files-from -' > $@

