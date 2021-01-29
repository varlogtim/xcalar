common_mk_dir := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
TOP        	  := $(patsubst %/,%,$(dir $(realpath $(firstword $(MAKEFILE_LIST)))))

export DC ?= xcalar-sjc
export DC_ENV = $(XLRDIR)/docker/mk/datacenter/$(DC).env

include $(common_mk_dir)/image.mk
include $(XLRDIR)/pkg/VERSIONS
include $(XLRDIR)/docker/mk/datacenter/$(DC).env
-include $(common_mk_dir)/local.mk
-include local.mk

NETWORK ?= xcalarnet
SUBNET 	?= 172.20.0.0/16
GATEWAY ?= 172.20.0.1

DOCKER_SOCKET ?= /var/run/docker.sock
SUDO   := $(shell test -w $(DOCKER_SOCKET) || echo sudo)
DOCKER ?= $(SUDO) docker
DOCKER_COMPOSE ?= $(SUDO) docker-compose

export CONTAINER_UID  := $(shell id -u)
export CONTAINER_GID  := $(shell id -g)
export CONTAINER_USER := $(shell test $(CONTAINER_UID) = 1000 && echo xcalardev || echo user$(CONTAINER_UID) )
export CONTAINER_HOME := /home/$(CONTAINER_USER)

no_proxy ?= localhost,127.0.0.1,.local,.localdomain,.int.xcalar.com,172.,192.168.

export NEXUS_HOST NEXUS_IP NETSTORE_IP NETSTORE_HOST XCALAR_REPOHOST XCALAR_REPOHOST_IP http_proxy https_proxy no_proxy

BUILD_NUMBER ?= 1
JOB_NAME     ?= local
VERSION	     := $(shell cat $(XLRDIR)/VERSION)
GIT_BRANCH   ?= HEAD

SHARED_CACHE = /var/opt/cache
BUILD        = el7-build
BUILD_AGENT  = el7-build-agent
VSTS_AGENT   = el7-vsts-agent
BUILD_NS     = xcalar/$(BUILD)
BUILD_AGENT_NS = xcalar/$(BUILD_AGENT)
VSTS_AGENT_NS  = xcalar/$(VSTS_AGENT)

V ?= 0
ifeq ($(V),0)
    Q = --quiet
    E = @
    QUIET = --quiet
    SILENT = --silent
    S = -s
    MAKEFLAGS= --silent
endif

BUILD_ARGS := $(shell cat $(XLRDIR)/pkg/VERSIONS $(DC_ENV) | sed 's/^/--build-arg=/' | tr '\n' ' ')
# These mess up the caching, only use for baking "final" images before push
BAKE_LABELS = \
    --label org.label-schema.build-date="$(shell date +%FT%T%z)" \
    --label org.label-schema.vcs-ref="$(shell git rev-parse --short $(GIT_BRANCH))" \
    --label com.xcalar.git-branch="$(GIT_BRANCH)" \
    --label com.xcalar.job-name="$(JOB_NAME)" \
    --label com.xcalar.git-describe="$(shell git describe --tags $(GIT_BRANCH))" \
    --label com.xcalar.build-number="$(BUILD_NUMBER)" \
    --label org.label-schema.build-date="$(shell date +%FT%T%z)"

LABELS = --label org.label-schema.schema-version="1.0" \
    --label org.label-schema.vendor="Xcalar, Inc" \
    --label org.label-schema.version="$(VERSION)" \
    --label org.label-schema.vcs-url="ssh://gerrit.int.xcalar.com:29418/xcalar.git" \
    --label org.label-schema.license="Proprietary"

OFFICIAL ?= 0
ifeq ($(OFFICIAL),1)
    LABELS += $(BAKE_LABELS)
endif

%/.docker: %/Dockerfile
	@echo "Building $(@D) ..."
	$(E)$(DOCKER) build $(Q) -t $(@D) -f $< \
		$(shell test "$(USE_CACHE_FROM)" = 1 && echo -n "--cache-from=registry.int.xcalar.com/xcalar/$(@D):$(VERSION)" || echo -n "") \
		--add-host $(XCALAR_REPOHOST):$(XCALAR_REPOHOST_IP) \
		--add-host $(NEXUS_HOST):$(NEXUS_IP) \
		--build-arg=http_proxy=$(http_proxy) \
		--build-arg=https_proxy=$(https_proxy) \
		--build-arg=no_proxy=$(no_proxy) \
		--build-arg=CONTAINER_USER=$(CONTAINER_USER) \
		--build-arg=CONTAINER_UID=$(CONTAINER_UID) \
		--build-arg=CONTAINER_GID=$(CONTAINER_GID) \
		--build-arg=CONTAINER_HOME=$(CONTAINER_HOME) \
		--build-arg=SHARED_CACHE=$(SHARED_CACHE) \
		--build-arg=CENTOS7=$(CENTOS7) \
		--build-arg=UB18=$(UB18) \
		--build-arg=AMZN2=$(AMZN2) \
		--build-arg=AMZN1=$(AMZN1) \
		--build-arg=EL7_BASE=el7-base:latest \
		--build-arg=EL7_BUILD=el7-build:latest \
		$(BUILD_ARGS) \
		$(LABELS) \
		--label org.label-schema.name="xcalar/$(@D)" \
		--ulimit core=0:0 $(DOCKER_BUILD_FLAGS) $(XLRDIR)

%-deps.tar: %-deps/.docker
	$(DOCKER) run --entrypoint=/bin/bash --rm $(^D) -c 'find . -mindepth 1 -maxdepth 1 -type f \( -name "*.deb" -o -name "*.rpm" \) | tar c --files-from -' > $@
