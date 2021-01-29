.PHONY: default up clean stop down logs network

include ../common.mk

SHELL=/bin/bash
ENV_FILE = docker_host.env
HOSTIP := $(shell $(DOCKER) run --network=host --rm centos:7 hostname -i)
DOCKER_HOST_IP := $(HOSTIP)
DOCKER_COMPOSE_CMD = $(DOCKER_COMPOSE) --env-file $(ENV_FILE)

export HOSTIP DOCKER_HOST_IP

default: up

network:
	make -C $(XLRDIR)/docker/xcalarnet

up: network
	echo "DOCKER_HOST_IP=$(HOSTIP)" > $(ENV_FILE)
	echo "HOSTIP=$(HOSTIP)" >> $(ENV_FILE)
	$(DOCKER_COMPOSE_CMD) pull
	$(DOCKER_COMPOSE_CMD) up -d
	sleep 3
	$(DOCKER_COMPOSE_CMD) logs --tail=200

logs:
	$(DOCKER_COMPOSE_CMD) logs -f --tail=200

stop: down
down:
	$(DOCKER_COMPOSE_CMD) down -v --remove-orphans

shell:
	$(DOCKER) exec -it kafka_kafka1_1 bash -l

clean: down
	rm -fv $(ENV_FILE)
