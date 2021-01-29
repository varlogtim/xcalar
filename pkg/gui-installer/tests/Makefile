.PHONY: all run hosts.txt build clean

OS ?= el7
NAME ?= install-target
IMAGE ?= $(NAME):$(OS)
DOCKER_ARGS=-d -t --dns 10.10.2.136 --dns 10.10.1.1 --dns-search int.xcalar.com --privileged -v /sys/fs/cgroup:/sys/fs/cgroup:ro

all: Dockerfile.$(OS) run hosts.txt

%: Dockerfile.%
	@echo Building $< ...
	@docker build --build-arg=http_proxy=$(http_proxy) -t $(NAME):$@ -f Dockerfile.$@ . >/dev/null

Dockerfile.%: header.% Dockerfile.common footer.%
	@cat $^ > $@

run: build clean
	@echo Launching 4 $(IMAGE) containers ...
	@docker run $(DOCKER_ARGS) -p 8888:80 --hostname target1 --name target1 $(IMAGE) >/dev/null
	@docker run $(DOCKER_ARGS) --link target1 --hostname target2 --name target2 $(IMAGE) >/dev/null
	@docker run $(DOCKER_ARGS) --link target1 --hostname target3 --name target3 $(IMAGE) >/dev/null
	@docker run $(DOCKER_ARGS) --link target1 --hostname target4 --name target4 $(IMAGE) >/dev/null

hosts.txt:
	@for ii in `seq 1 4`; do docker inspect --format '{{ .NetworkSettings.IPAddress }}' target$$ii; done | tee $@

build: ub14 el7

clean:
	@echo Removing old containers ...
	-@docker ps -a | grep target[1-4] | awk '{print $$1}' | xargs -r docker stop >/dev/null
	-@docker ps -a | grep target[1-4] | awk '{print $$1}' | xargs -r docker rm -fv >/dev/null
	@rm -f Dockerfile.ub14 Dockerfile.el?
