.PHONY: all build psuh

all: build

build:
	docker build --build-arg http_proxy=$(http_proxy) --build-arg CENTOS7=$(CENTOS7) -t fpm:latest .

push: build
	docker tag fpm:latest $(FPM)
	docker push $(FPM)

include ../common.mk
-include ../local.mk
