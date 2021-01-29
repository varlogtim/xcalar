.PHONY: all build run stop rm shell clean
SHELL:=/bin/bash
HOSTNAME_F  := $(shell hostname -f)
HOSTNAME_S  := $(shell hostname -s)
DNSDOMAIN := $(shell dnsdomainname)
NAME  = hdfs_hdfs_sanity_1
IMAGE = hdfs_sanity
NOCACHE ?= false
QUIET ?= -q
CONTAINER_USER:=$(shell id -un)
CONTAINER_UID:=$(shell id -u)
HOSTPORTS = -p 2122:2122 -p 8020:9000 -p 8088:8088 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075
LOCALPORTS = -p 127.0.0.1:2122:2122 -p 127.0.0.1:8020:9000 -p 127.0.0.1:8088:8088 -p 127.0.0.1:50010:50010 -p 127.0.0.1:50020:50020 -p 127.0.0.1:50070:50070 -p 127.0.0.1:50075:50075
PORTS = $(HOSTPORTS)
RUN_ARGS = -d --restart=unless-stopped --name $(NAME) --ulimit nofile=64960 \
        -e "VIRTUAL_HOST=$(HOSTNAME_F),$(HOSTNAME_S)" --hostname hdfs-sanity -e VIRTUAL_PORT=8088 $(PORTS)


all: run

DOCKER_COMPOSE_VERSION = 1.8.0
DOCKER_COMPOSE = /usr/local/bin/docker-compose
.PRECIOUS: %/docker-compose-$(DOCKER_COMPOSE_VERSION)

%/docker-compose-$(DOCKER_COMPOSE_VERSION):
	mkdir -p $(@D)
	curl -sSL https://github.com/docker/compose/releases/download/$(DOCKER_COMPOSE_VERSION)/docker-compose-`uname -s`-`uname -m` > $@.tmp
	chmod +x $@.tmp
	mv $@.tmp $@


%/docker-compose: %/docker-compose-$(DOCKER_COMPOSE_VERSION)
	ln -sfn $< $@

build:
	@echo Building Docker $(IMAGE) image
	@docker build $(QUIET) --build-arg=http_proxy=$(http_proxy) --no-cache=$(NOCACHE) -t $(IMAGE) .

run: build
	@test "`docker inspect --format '{{.Id}}' $(IMAGE) 2>/dev/null`" = "`docker inspect --format '{{.Image}}' $(NAME) 2>/dev/null`" || { docker rm -fv $(NAME) || true; }
	@test "`docker inspect --format '{{.State.Running}}' $(NAME) 2>/dev/null`" = true || { \
        i=0 && \
        docker stop -t 10 $(NAME) 2>/dev/null || true; \
        docker rm -fv $(NAME) 2>/dev/null || true; \
        docker run $(RUN_ARGS) $(IMAGE) && \
        echo "Waiting for $(NAME) to start. This may take a minute during which time you will see some errors." && \
        sleep 2 && \
        until docker exec $(NAME) bin/hdfs dfs -touchz /dfsSentinel; do \
            echo "[$$i] Waiting..."; \
            i=$$(( i + 1 )); \
            if test $$i -gt 20; then \
                echo >&2 "Timed out waiting for $(NAME) ..."; \
                exit 1; \
            fi; \
            sleep 5; \
        done && \
        docker exec $(NAME) bin/hdfs dfs -rm /dfsSentinel && \
        docker exec $(NAME) bash -c "test -d /home/$(CONTAINER_USER) || useradd --non-unique -m -s /bin/bash -u $(CONTAINER_UID) $(CONTAINER_USER)" && \
        docker exec $(NAME) bin/hdfs dfs -mkdir -p /user/$(CONTAINER_USER)/export && \
        docker exec $(NAME) bin/hdfs dfs -chown -R $(CONTAINER_USER) /user/$(CONTAINER_USER) && \
        docker exec $(NAME) bin/hdfs dfs -rm -R /exports && \
        docker exec $(NAME) bin/hdfs dfs -mkdir -p /exports && \
        docker exec $(NAME) bin/hdfs dfs -chmod 0777 /exports || { docker rm -fv $(NAME) || true; }; }

stop:
	-docker stop -t 10 $(NAME)

shell:
	docker exec -ti $(NAME) bash -l

rm: stop
	-docker rm -fv $(NAME)

clean: rm
	docker rmi $(IMAGE)
