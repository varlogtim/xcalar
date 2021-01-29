.PHONY: default all run build push shell kill

SHELL=/bin/bash
AGENT_NAME ?= $(shell hostname -s)-agent
AGENT_WORKDIR = /azp
tmpFsSizeKb = $(shell grep MemTotal /proc/meminfo | awk '{ printf "%.0f\n", $$2*95/100}' | sed -e 's/\.*$$//')
CONTAINER ?= vsts-agent

AGENT_IMAGE = el7-vsts-agent
REGISTRY_IMAGE = registry.service.consul/xcalar/$(AGENT_IMAGE)

DOCKER ?= docker
DOCKER_ARGS = --privileged -v /sys/fs/cgroup:/sys/fs/cgroup:ro -e container=docker \
	--dns 10.10.2.136 \
	--dns 10.10.1.1 \
	--dns-search int.xcalar.com \
    --ulimit core=0:0 \
    --ulimit nofile=64960 \
    --ulimit nproc=140960:140960 \
    --ulimit memlock=-1:-1 \
    --ulimit stack=-1:-1 \
    --shm-size=$(tmpFsSizeKb)k \
    --memory-swappiness=10 \
	-e DOCKER_HOST=unix:///var/run/docker.sock \
	-v /var/run/docker.sock:/var/run/docker.sock \
	--tmpfs /run \
	-v $(AGENT_NAME)-volume:$(AGENT_WORKDIR)

default: build

all: build push run

build:
	$(MAKE) -C ..

push: build
	$(DOCKER) tag $(AGENT_IMAGE):latest $(REGISTRY_IMAGE)
	$(DOCKER) push $(REGISTRY_IMAGE)

run:
	$(DOCKER) run -d --name $(CONTAINER) $(DOCKER_ARGS) -e AZP_URL=https://dev.azure.com/xcalar -e AZP_TOKEN=`vault kv  get -field=token secret/infra/vsts` -e AZP_AGENT_NAME=$(AGENT_NAME) $(AGENT_IMAGE)
	@sleep 2
	@$(DOCKER) logs $(CONTAINER)
	@echo "$(CONTAINER) is running!"

shell:
	$(DOCKER) exec -u $(JENKINS_USER) -it $(CONTAINER) /bin/bash -l

kill:
	-$(DOCKER) rm -f $(CONTAINER) || true

include $(XLRDIR)/docker/common.mk
-include ../local.mk

