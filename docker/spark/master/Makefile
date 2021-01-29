.PHONY: all build run stop rm shell clean
SHELL := /bin/bash
REGISTRY := registry.service.consul
NAME := spark-master-jdbc
TAG := 2.3.2-hadoop2.7
IMAGE := $(REGISTRY)/$(NAME):$(TAG)


build: Dockerfile master.sh
	@echo Building Docker $(IMAGE) image
	@docker build -t $(IMAGE) .

run: build
	docker run --init -v /netstore:/netstore --name $(NAME) -P -e ENABLE_INIT_DAEMON=false -d $(IMAGE)

stop:
	-docker stop -t 10 $(NAME)

shell:
	docker exec -it $(NAME) /bin/bash

rm: stop
	-docker rm -fv $(NAME)
	sleep 1

clean: rm
	-docker rmi $(IMAGE)
