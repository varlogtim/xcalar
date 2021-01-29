.PHONY: image stop run logs logsf xccli shell
SHELL = /bin/bash

IMAGE ?= xcalar
NAME  ?= xcalar-0
TZ  ?= America/Los_Angeles
# PLEASE DONT USE THE SYMLINKS (like xcalar-latest, etc), or the image will not r.tmp/build
INSTALLER_URL ?= http://netstore.int.xcalar.com/builds/Release/xcalar-1.3.1/prod/xcalar-1.3.1-1742-installer
DEFAULT_CFG_PATH ?= default.cfg
SHARE = xcalar_root
REGISTRY = registry.int.xcalar.com/xcalar
http_proxy ?= http://cacher:3128
DOCKER_BUILD_FLAGS = --build-arg=http_proxy=$(http_proxy) --ulimit core=0:0
DOCKER_BUILD_EXTRA_FLAGS =
XCCLI_ARGS = -c version
ARGS = --cap-add=ALL --cap-drop=MKNOD \
       --security-opt seccomp:unconfined \
       --ulimit core=-1:-1 \
       --ulimit nofile=64960 \
       --ulimit nproc=50960:50960 \
       --ulimit memlock=-1:-1 \
       --ulimit stack=-1:-1 \
	   -p 443:8443 \
	   -v $(SHARE):/var/opt/xcalar \
       --name $(NAME) \
       --hostname $(NAME) \
       $(IMAGE)

all: image push

image: $(IMAGE).tar.gz

build:
	docker inspect xcalar-el7-base:latest >/dev/null || \
		docker build $(DOCKER_BUILD_FLAGS) $(DOCKER_BUILD_EXTRA_FLAGS) -t xcalar-el7-base -f xcalar-el7-base.df .
	docker build --build-arg=INSTALLER_URL=$(INSTALLER_URL) $(DOCKER_BUILD_FLAGS) $(DOCKER_BUILD_EXTRA_FLAGS) -t $(IMAGE) .
	TAG=$$(docker run -ti --rm --entrypoint=/bin/rpm $(IMAGE) -q xcalar --qf '%{VERSION}-%{RELEASE}') && \
		docker rmi $(IMAGE):$${TAG}; docker tag $(IMAGE) $(IMAGE):$${TAG}

$(IMAGE).tar.gz: build
	TAG=$$(docker run -ti --rm --entrypoint=/bin/rpm $(IMAGE) -q xcalar --qf '%{VERSION}-%{RELEASE}') && \
		time docker save $(IMAGE):latest $(IMAGE):$${TAG} | pigz > $(IMAGE)-$${TAG}.tar.gz

stop:
	docker stop $(NAME) 2>/dev/null || true

push: build
	TAG=$$(docker run -ti --rm --entrypoint=/bin/rpm $(IMAGE) -q xcalar --qf '%{VERSION}-%{RELEASE}') && \
		docker rmi $(REGISTRY)/$(IMAGE):latest $(REGISTRY)/$(IMAGE):$${TAG} || true; \
		docker tag $(IMAGE) $(REGISTRY)/$(IMAGE):latest && \
		docker tag $(IMAGE) $(REGISTRY)/$(IMAGE):$${TAG} && \
		docker push $(REGISTRY)/$(IMAGE):$${TAG} && \
		docker push $(REGISTRY)/$(IMAGE):latest

run: build stop
	docker run -d -t $(ARGS)

runWithCfg: build stop clean
	docker run -d -t -v $(DEFAULT_CFG_PATH):/etc/xcalar/default.cfg $(ARGS)

clean: stop
	docker ps -a | grep $(NAME)- | awk '{print $$1}' | xargs -r docker rm -f || true
	docker rmi $(IMAGE)
	rm -f $(IMAGE)-*.tar.bz2 $(IMAGE)-*.tar.gz

logs:
	docker logs --tail=100 -f $(NAME)

xccli:
	docker exec -ti $(NAME) /opt/xcalar/bin/xccli $(XCCLI_ARGS)

shell:
	docker exec -ti $(NAME) /bin/bash -l
