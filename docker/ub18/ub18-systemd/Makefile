

TAG ?= v1
IMAGE = ub18-systemd:$(TAG)
REGISTRY = registry.int.xcalar.com/xcalar/ub18-systemd:$(TAG)

all:
	docker build --label com.xcalar.ctr-command="docker run --rm --privileged -v /:/host" -t $(IMAGE) .

push: all
	docker tag $(IMAGE) $(REGISTRY)
	docker push $(REGISTRY)

pull:
	docker pull $(REGISTRY)
	docker tag $(REGISTRY) $(IMAGE)

run:
	docker run --rm --privileged -v /:/host $(IMAGE) setup
