.PHONY: all ub14-build

IMAGE = $(UB14)

include ../common.mk

all: ub14-build/.docker

ub14-deps/.docker: ub14-deps/Dockerfile
	tar cf - -C $(XLRDIR) docker/ub14/ub14-deps/Dockerfile $$(grep -E '(ADD|COPY)' ub14-deps/Dockerfile | awk '{print $$2}' | sort | uniq) | \
    docker build -t $(@D) -f docker/ub14/$< \
       --build-arg=XCALAR_PYTHON_VERSION=$(XCALAR_PYTHON_VERSION) \
       --build-arg=IMAGE=$(UB14) \
       --build-arg=http_proxy=$(http_proxy) -
