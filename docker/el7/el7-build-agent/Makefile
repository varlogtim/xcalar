TOP  := $(patsubst %/,%,$(dir $(realpath $(firstword $(MAKEFILE_LIST)))))

.PHONY: all run
include swarm.env

SHELL=/bin/bash
JENKINS_AGENT_NAME ?= $(shell id -un)-$(shell hostname -s)-swarm
tmpFsSizeKb = $(shell grep MemTotal /proc/meminfo | awk '{ printf "%.0f\n", $$2*95/100}' | sed -e 's/\.*$$//')
CONTAINER ?= $(JENKINS_AGENT_NAME)
CONTAINER_UID := $(shell id -u)
CONTAINER_GID := $(shell id -g)
export CONTAINER_USER CONTAINER_HOME JENKINS_HOME
CACHEVOL_VERSION ?= 3
CACHEVOL := u$(shell id -u)_cache_v$(CACHEVOL_VERSION)
CONTAINER_HOMEVOL := u$(shell id -u)_homevol_v$(CACHEVOL_VERSION)
CIDFILE = $(TOP)/swarm/agent.cid
CONTAINER_ID := $(shell cat $(CIDFILE) 2>/dev/null || true)
VERSION := $(shell cat $(XLRDIR)/VERSION)
export CACHEVOL CONTAINER_HOMEVOL
DOCKER_SOCKET ?= /var/run/docker.sock
SUDO   := $(shell test -w $(DOCKER_SOCKET) || echo sudo)
DOCKER ?= $(SUDO) docker

DOCKER_ARGS = \
	-t \
	--dns 10.10.2.136 \
	--dns-search int.xcalar.com \
	--hostname $(CONTAINER) \
	--domainname int.xcalar.com \
	--cap-add CAP_SYS_PTRACE \
	--cap-add CAP_IPC_LOCK \
	--security-opt seccomp=unconfined \
	--ulimit core=-1:-1 \
	--ulimit nofile=140960:140960 \
	--ulimit nproc=140960:140960 \
	--ulimit memlock=-1:-1 \
	--ulimit stack=-1:-1 \
	--shm-size=$(tmpFsSizeKb)k \
	--memory-swappiness=10 \
	-e container=docker \
	-e HOST=$(shell hostname -f) \
	-e HOSTIP=$(shell get_local_ip.sh) \
	-e HOSTOS=$(shell osid) \
	-e HOSTUSER=$(shell id -un) \
	-e CONTAINER_USER=$(CONTAINER_USER) \
	-e CONTAINER_HOME=$(CONTAINER_HOME) \
	-e CONTAINER_UID=$(CONTAINER_UID) \
	-e CONTAINER_GID=$(CONTAINER_GID) \
	-e CIDFILE=$(CIDFILE) \
	-e NETSTORE_NFS=/netstore \
	-e JENKINS_AGENT_NAME=$(JENKINS_AGENT_NAME) \
	--env-file $(TOP)/swarm.env \
	--cidfile $(CIDFILE) \
	-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v /netstore:/netstore \
	-v $(CONTAINER_HOMEVOL):$(JENKINS_HOME) \
	-v $(CACHEVOL):$(SHARED_CACHE) \
	-v $(shell pwd)/swarm:/swarm:ro \
	-w $(JENKINS_AGENT_WORKDIR) \
	--tmpfs /run \
	--tmpfs /run/lock

V ?= 0

ifeq ($(V),0)
	Q = -q
	E = @
endif


include ../../common.mk

IMAGE = el7-build-agent

default: up

up: build clean start

build:
	$(E)echo Building images ...
	$(E)$(DOCKER) build $(Q) -t el7-build-agent -f Dockerfile \
		--add-host netstore:$(NETSTORE_IP) \
		--add-host nexus.int.xcalar.com:$(NOMAD_IP) \
		--build-arg=EL7_MIRROR_URL=http://$(NETSTORE_IP)/isos/centos/7 \
		--build-arg=CONTAINER_USER=$(JENKINS_USER) \
		--build-arg=CONTAINER_UID=$(CONTAINER_UID) \
		--build-arg=CONTAINER_HOME=$(CONTAINER_HOME) \
		--build-arg=JENKINS_HOME=$(JENKINS_HOME) \
		--build-arg=SHARED_CACHE=$(SHARED_CACHE) \
		--build-arg=EL7_BASE=el7-base:latest \
		--build-arg=EL7_BUILD=el7-build:latest \
		--build-arg=XCALAR_REPOPKG_ORG="http://repo.xcalar.net" \
		--build-arg=XCALAR_REPOPKG_URL="http://repo.xcalar.net" \
		--label org.label-schema.schema-version="1.0" --label org.label-schema.vendor="Xcalar, Inc" --label org.label-schema.version="$(VERSION)" --label org.label-schema.vcs-url="ssh://gerrit.int.xcalar.com:29418/xcalar.git" --label org.label-schema.license="Proprietary" \
		--label org.label-schema.name="xcalar/el7-build-agent" \
		--ulimit core=0:0 $(XLRDIR)

swarm/user:
	$(E)vault read -field=display_name -format=table auth/token/lookup-self > $@.tmp \
		|| { echo >&2 "Failed to get vault login info. Please login to vault!";  \
			vault read -field=display_name -format=table auth/token/lookup-self; \
			exit 1; \
		   }
	@mv $@.tmp $@

swarm/swarmpass: swarm/user
	@mkdir -p $(@D)
	$(E)vault kv  get -field=token secret/roles/jenkins-slave/jenkins-swarm > $@.tmp
	@mv $@.tmp $@

swarm/swarmlabels:
	@mkdir -p $(@D)
	$(E)echo "`id -un` `hostname -s`" > $@

start: swarm/swarmlabels swarm/swarmpass
	@mkdir -p $(@D)
	$(E)$(DOCKER) run -d --name $(CONTAINER) $(DOCKER_ARGS) -- $(IMAGE)
	@sleep 3
	$(E)$(DOCKER) exec -d -e RUNAS=$(CONTAINER_UID) -- `cat $(CIDFILE)` /docker-entrypoint.sh jenkins-swarm
	@echo "$(CONTAINER) is running!"

shell:
	$(E)$(DOCKER) exec -u $(CONTAINER_UID) -it -- $(CONTAINER_ID) /bin/bash -l

rootsh:
	$(E)$(DOCKER) exec -it $(CONTAINER) /bin/bash -l

kill:
	-$(E)test -n "$(CONTAINER_ID)" && $(DOCKER) kill $(CONTAINER_ID) || true

clean:
	$(E)test -n "$(CONTAINER_ID)" && $(DOCKER) rm -f -v $(CONTAINER_ID) || true
	$(E)rm -fv $(CIDFILE)

really-clean: clean
	-$(DOCKER) volume rm -f $(CACHEVOL) $(CONTAINER)_home
	-$(DOCKER) rmi el7-build-agent:latest
